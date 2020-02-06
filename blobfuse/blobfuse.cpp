#include "blobfuse.h"
#include <boost/filesystem.hpp>
#include <string>

namespace {
    std::string trim(const std::string& str) {
        const size_t start = str.find_first_not_of(' ');
        if (std::string::npos == start) {
            return std::string();
        }
        const size_t end = str.find_last_not_of(' ');
        return str.substr(start, end - start + 1);
    }
}

// FUSE contains a specific type of command-line option parsing; here we are just following the pattern.
struct options
{
    const char *tmp_path; // Path to the temp / file cache directory
    const char *config_file; // Connection to Azure Storage information (account name, account key, etc)
    const char *use_https; // True if https should be used (defaults to false)
    const char *file_cache_timeout_in_seconds; // Timeout for the file cache (defaults to 120 seconds)
    const char *container_name; //container to mount. Used only if config_file is not provided
    const char *log_level; // Sets the level at which the process should log to syslog.
    const char *use_attr_cache; // True if the cache for blob attributes should be used.
    const char *use_adls; // True if the dfs/DataLake endpoint should be used when necessary
    const char *version; // print blobfuse version
    const char *help; // print blobfuse usage
};

struct options file_options;

#define OPTION(t, p) { t, offsetof(struct options, p), 1 }
const struct fuse_opt option_spec[] =
{
    OPTION("--tmp-path=%s", tmp_path),
    OPTION("--config-file=%s", config_file),
    OPTION("--use-https=%s", use_https),
    OPTION("--file-cache-timeout-in-seconds=%s", file_cache_timeout_in_seconds),
    OPTION("--container-name=%s", container_name),
    OPTION("--log-level=%s", log_level),
    OPTION("--use-attr-cache=%s", use_attr_cache),
    OPTION("--use-adls=%s", use_adls),
    OPTION("--version", version),
    OPTION("-v", version),
    OPTION("--help", help),
    OPTION("-h", help),
    FUSE_OPT_END
};

const std::string log_ident = "blobfuse";

inline bool is_lowercase_string(const std::string &s)
{
    return (s.size() == static_cast<size_t>(std::count_if(s.begin(), s.end(),[](unsigned char c)
    {
        return std::islower(c);
    })));
}

std::string to_lower(std::string original) {
    std::string out;

    for (auto idx = original.begin(); idx < original.end(); idx++) {
        if(*idx >= 'A' && *idx <= 'Z') {
            out += char(*idx + 32); // This cast isn't required, but clang-tidy wants to complain without it.
        } else {
            out += *idx;
        }
    }

    return out;
}

AUTH_TYPE get_auth_type(std::string authStr) {

    if(!authStr.empty()) {
        std::string lcAuthType = to_lower(authStr);
        if (lcAuthType == "msi") {
            // MSI does not require any parameters to work, asa a lone system assigned identity will work with no parameters.
            return MSI_AUTH;
        } else if (lcAuthType == "key") {
            if(!str_options.accountKey.empty()) // An account name is already expected to be specified.
                return KEY_AUTH;
            else
                return INVALID_AUTH;
        } else if (lcAuthType == "sas") {
            if (!str_options.sasToken.empty()) // An account name is already expected to be specified.
                return SAS_AUTH;
            else
                return INVALID_AUTH;
        }
    } else {
        if (!str_options.objectId.empty() || !str_options.clientId.empty() || !str_options.resourceId.empty()) {
            return MSI_AUTH;
        } else if (!str_options.accountKey.empty()) {
            return KEY_AUTH;
        } else if (!str_options.sasToken.empty()) {
            return SAS_AUTH;
        }
    }
    return INVALID_AUTH;
}

// Read Storage connection information from the environment variables
int read_config_env()
{
    char* env_account = getenv("AZURE_STORAGE_ACCOUNT");
    char* env_account_key = getenv("AZURE_STORAGE_ACCESS_KEY");
    char* env_sas_token = getenv("AZURE_STORAGE_SAS_TOKEN");
    char* env_blob_endpoint = getenv("AZURE_STORAGE_BLOB_ENDPOINT");
    char* env_identity_client_id = getenv("AZURE_STORAGE_IDENTITY_CLIENT_ID");
    char* env_identity_object_id = getenv("AZURE_STORAGE_IDENTITY_OBJECT_ID");
    char* env_identity_resource_id = getenv("AZURE_STORAGE_IDENTITY_RESOURCE_ID");
    char* env_managed_identity_endpoint = getenv("AZURE_STORAGE_MANAGED_IDENTITY_ENDPOINT");
    char* env_auth_type = getenv("AZURE_STORAGE_AUTH_TYPE");

    if(env_account)
    {
        str_options.accountName = env_account;

        if(env_account_key)
        {
            str_options.accountKey = env_account_key;
        }

        if(env_sas_token)
        {
            str_options.sasToken = env_sas_token;
        }

        if(env_identity_client_id)
        {
            str_options.clientId = env_identity_client_id;
        }

        if(env_identity_object_id)
        {
            str_options.objectId = env_identity_object_id;
        }

        if(env_identity_resource_id)
        {
            str_options.resourceId = env_identity_resource_id;
        }

        if(env_managed_identity_endpoint)
        {
            str_options.msiEndpoint = env_managed_identity_endpoint;
        }

        if(env_auth_type)
        {
            str_options.authType = get_auth_type(env_auth_type);
        }

        if(env_blob_endpoint) {
            // Optional to specify blob endpoint
            str_options.blobEndpoint = env_blob_endpoint;
        }
    }
    else
    {
        syslog(LOG_CRIT, "Unable to start blobfuse.  No config file was specified and the AZURE_STORAGE_ACCCOUNT"
                         "environment variable was empty");
        fprintf(stderr, "Unable to start blobfuse.  No config file was specified and the AZURE_STORAGE_ACCCOUNT"
                        "environment variable was empty\n");
        return -1;
    }

    return 0;
}

// Read Storage connection information from the config file
int read_config(const std::string configFile)
{
    std::ifstream file(configFile);
    if(!file)
    {
        syslog(LOG_CRIT, "Unable to start blobfuse.  No config file found at %s.", configFile.c_str());
        fprintf(stderr, "No config file found at %s.\n", configFile.c_str());
        return -1;
    }

    std::string line;
    std::istringstream data;

    while(std::getline(file, line))
    {

        data.str(line.substr(line.find(" ")+1));
        const std::string value(trim(data.str()));

        if(line.find("accountName") != std::string::npos)
        {
            std::string accountNameStr(value);
            str_options.accountName = accountNameStr;
        }
        else if(line.find("accountKey") != std::string::npos)
        {
            std::string accountKeyStr(value);
            str_options.accountKey = accountKeyStr;
        }
        else if(line.find("sasToken") != std::string::npos)
        {
	        std::string sasTokenStr(value);
	        str_options.sasToken = sasTokenStr;
        }
        else if(line.find("containerName") != std::string::npos)
        {
            std::string containerNameStr(value);
            str_options.containerName = containerNameStr;
        }
        else if(line.find("blobEndpoint") != std::string::npos)
        {
            std::string blobEndpointStr(value);
            str_options.blobEndpoint = blobEndpointStr;
        }
        else if(line.find("identityClientId") != std::string::npos)
        {
            std::string clientIdStr(value);
            str_options.clientId = clientIdStr;
        }
        else if(line.find("identityObjectId") != std::string::npos)
        {
            std::string objectIdStr(value);
            str_options.objectId = objectIdStr;
        }
        else if(line.find("identityResourceId") != std::string::npos)
        {
            std::string resourceIdStr(value);
            str_options.resourceId = resourceIdStr;
        }
        else if(line.find("authType") != std::string::npos)
        {
            str_options.authType = get_auth_type(value);
        }
        else if(line.find("msiEndpoint") != std::string::npos)
        {
            std::string msiEndpointStr(value);
            str_options.msiEndpoint = msiEndpointStr;
        }

        data.clear();
    }

    if(str_options.accountName.empty())
    {
        syslog (LOG_CRIT, "Unable to start blobfuse. Account name is missing in the config file.");
        fprintf(stderr, "Account name is missing in the config file.\n");
        return -1;
    }
    else if(str_options.containerName.empty())
    {
        syslog (LOG_CRIT, "Unable to start blobfuse. Container name is missing in the config file.");
        fprintf(stderr, "Container name is missing in the config file.\n");
        return -1;
    }
    else
    {
        return 0;
    }
}


void *azs_init(struct fuse_conn_info * conn)
{
    if (file_options.use_adls != NULL)
    {
        if (strcmp(file_options.use_adls, "true"))
        {
            storage_client = std::make_shared<DataLakeBfsClient>(str_options);
        }
        else
        {
            //TODO:There's probably a way to not have this and have it skip to the other else.. I'm forgetting though
            storage_client = std::make_shared<BlockBlobBfsClient>(str_options);
        }
    }
    else
    {
        storage_client = std::make_shared<BlockBlobBfsClient>(str_options);
    }
    if(storage_client->AuthenticateStorage())
    {
        syslog(LOG_DEBUG, "Successfully Authenticated!");
    }
    else
    {
        syslog(LOG_ERR, "Unable to start blobfuse due to a lack of credentials. Please check the readme for valid auth setups.");
        azs_destroy(NULL);
        return NULL;
    }
    /*
    cfg->attr_timeout = 360;
    cfg->kernel_cache = 1;
    cfg->entry_timeout = 120;
    cfg->negative_timeout = 120;
    */
    conn->max_write = 4194304;
    //conn->max_read = 4194304;
    conn->max_readahead = 4194304;
    conn->max_background = 128;
    //  conn->want |= FUSE_CAP_WRITEBACK_CACHE | FUSE_CAP_EXPORT_SUPPORT; // TODO: Investigate putting this back in when we downgrade to fuse 2.9

    g_gc_cache = std::make_shared<gc_cache>(str_options.tmpPath, str_options.fileCacheTimeoutInSeconds);
    g_gc_cache->run();

    return NULL;
}

// TODO: print FUSE usage as well
void print_usage()
{
    fprintf(stdout, "Usage: blobfuse <mount-folder> --tmp-path=</path/to/fusecache> [--config-file=</path/to/config.cfg> | --container-name=<containername>]");
    fprintf(stdout, "    [--use-https=true] [--file-cache-timeout-in-seconds=120] [--log-level=LOG_OFF|LOG_CRIT|LOG_ERR|LOG_WARNING|LOG_INFO|LOG_DEBUG] [--use-attr-cache=true]\n\n");
    fprintf(stdout, "In addition to setting --tmp-path parameter, you must also do one of the following:\n");
    fprintf(stdout, "1. Specify a config file (using --config-file]=) with account name, account key, and container name, OR\n");
    fprintf(stdout, "2. Set the environment variables AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_ACCESS_KEY, and specify the container name with --container-name=\n\n");
    fprintf(stdout, "See https://github.com/Azure/azure-storage-fuse for detailed installation and configuration instructions.\n");
}

void print_version()
{
    fprintf(stdout, "blobfuse 1.2.2\n");
}

int set_log_mask(const char * min_log_level_char)
{
    if (!min_log_level_char)
    {
        setlogmask(LOG_UPTO(LOG_WARNING));
        return 0;
    }
    std::string min_log_level(min_log_level_char);
    if (min_log_level.empty())
    {
        setlogmask(LOG_UPTO(LOG_WARNING));
        return 0;
    }
    // Options for logging: LOG_OFF, LOG_CRIT, LOG_ERR, LOG_WARNING, LOG_INFO, LOG_DEBUG
    if (min_log_level == "LOG_OFF")
    {
        setlogmask(LOG_UPTO(LOG_EMERG)); // We don't use 'LOG_EMERG', so this won't log anything.
        return 0;
    }
    if (min_log_level == "LOG_CRIT")
    {
        setlogmask(LOG_UPTO(LOG_CRIT));
        return 0;
    }
    if (min_log_level == "LOG_ERR")
    {
        setlogmask(LOG_UPTO(LOG_ERR));
        return 0;
    }
    if (min_log_level == "LOG_WARNING")
    {
        setlogmask(LOG_UPTO(LOG_WARNING));
        return 0;
    }
    if (min_log_level == "LOG_INFO")
    {
        setlogmask(LOG_UPTO(LOG_INFO));
        return 0;
    }
    if (min_log_level == "LOG_DEBUG")
    {
        setlogmask(LOG_UPTO(LOG_DEBUG));
        return 0;
    }

    syslog(LOG_CRIT, "Unable to start blobfuse. Error: Invalid log level \"%s\"", min_log_level.c_str());
    fprintf(stdout, "Error: Invalid log level \"%s\".  Permitted values are LOG_OFF, LOG_CRIT, LOG_ERR, LOG_WARNING, LOG_INFO, LOG_DEBUG.\n", min_log_level.c_str());
    fprintf(stdout, "If not specified, logging will default to LOG_WARNING.\n\n");
    return 1;
}

void set_up_callbacks(fuse_operations &azs_blob_operations)
{
    openlog(log_ident.c_str(), LOG_NDELAY | LOG_PID, 0);

    // Here, we set up all the callbacks that FUSE requires.
    azs_blob_operations.init = azs_init;
    azs_blob_operations.getattr = azs_getattr;
    azs_blob_operations.statfs = azs_statfs;
    azs_blob_operations.access = azs_access;
    azs_blob_operations.readlink = azs_readlink;
    azs_blob_operations.readdir = azs_readdir;
    azs_blob_operations.open = azs_open;
    azs_blob_operations.read = azs_read;
    azs_blob_operations.release = azs_release;
    azs_blob_operations.fsync = azs_fsync;
    azs_blob_operations.create = azs_create;
    azs_blob_operations.write = azs_write;
    azs_blob_operations.mkdir = azs_mkdir;
    azs_blob_operations.unlink = azs_unlink;
    azs_blob_operations.rmdir = azs_rmdir;
    azs_blob_operations.chown = azs_chown;
    azs_blob_operations.chmod = azs_chmod;
    //#ifdef HAVE_UTIMENSAT
    azs_blob_operations.utimens = azs_utimens;
    //#endif
    azs_blob_operations.destroy = azs_destroy;
    azs_blob_operations.truncate = azs_truncate;
    azs_blob_operations.rename = azs_rename;
    azs_blob_operations.setxattr = azs_setxattr;
    azs_blob_operations.getxattr = azs_getxattr;
    azs_blob_operations.listxattr = azs_listxattr;
    azs_blob_operations.removexattr = azs_removexattr;
    azs_blob_operations.flush = azs_flush;
}

int read_and_set_arguments(int argc, char *argv[], struct fuse_args *args)
{
    // FUSE has a standard method of argument parsing, here we just follow the pattern.
    *args = FUSE_ARGS_INIT(argc, argv);

    // Check for existence of allow_other flag and change the default permissions based on that
    str_options.defaultPermission = 0770;
    std::vector<std::string> string_args(argv, argv+argc);
    for (size_t i = 1; i < string_args.size(); ++i) {
      if (string_args[i].find("allow_other") != std::string::npos) {
          str_options.defaultPermission = 0777;
      }
    }

    int ret = 0;
    try
    {

        if (fuse_opt_parse(args, &file_options, option_spec, NULL) == -1)
        {
            return 1;
        }

        if(file_options.version)
        {
            print_version();
            exit(0);
        }

        if(file_options.help)
        {
            print_usage();
            exit(0);
        }

        if(!file_options.config_file)
        {
            if(!file_options.container_name)
            {
                syslog(LOG_CRIT, "Unable to start blobfuse, no config file provided and --container-name is not set.");
                fprintf(stderr, "Error: No config file provided and --container-name is not set.\n");
                print_usage();
                return 1;
            }

            std::string container(file_options.container_name);
            str_options.containerName = container;
            ret = read_config_env();
        }
        else
        {
            ret = read_config(file_options.config_file);
        }

        if (ret != 0)
        {
            return ret;
        }
    }
    catch(std::exception &)
    {
        print_usage();
        return 1;
    }

    int res = set_log_mask(file_options.log_level);
    if (res != 0)
    {
        print_usage();
        return 1;
    }

    // remove last trailing slash in tmp_path
    if(!file_options.tmp_path)
    {
        fprintf(stderr, "Error: --tmp-path is not set.\n");
        print_usage();
        return 1;
    }

    std::string tmpPathStr(file_options.tmp_path);
    if (!tmpPathStr.empty())
    {
        // First let's normalize the path
        // Don't use canonical because that will check for path existence and permissions
        tmpPathStr = boost::filesystem::path(tmpPathStr).lexically_normal().string();

        // Double check that we have not just emptied this string
        if (!tmpPathStr.empty())
        {
            // Trim any trailing '/' or '/.'
            // This will also create a blank string for just '/' which will fail out at the next block
            // .lexically_normal() returns '/.' for directories
            if (tmpPathStr[tmpPathStr.size() - 1] == '/')
            {
                tmpPathStr.erase(tmpPathStr.size() - 1);
            }
            else if (tmpPathStr.size() > 1 && tmpPathStr.compare((tmpPathStr.size() - 2), 2, "/.") == 0)
            {
                tmpPathStr.erase(tmpPathStr.size() - 2);
            }
        }

        // Error out if we emptied this string
        if (tmpPathStr.empty())
        {
            fprintf(stderr, "Error: --tmp-path resolved to empty path.\n");
            print_usage();
            return 1;
        }
    }

    str_options.tmpPath = tmpPathStr;
    str_options.useHttps = true;
    if (file_options.use_https != NULL)
    {
        std::string https(file_options.use_https);
        if (https == "false")
        {
            str_options.useHttps = false;
        }
    }

    str_options.useAttrCache = false;
    if (file_options.use_attr_cache != NULL)
    {
        std::string attr_cache(file_options.use_attr_cache);
        if (attr_cache == "true")
        {
            str_options.useAttrCache = true;
        }
    }

    if (file_options.file_cache_timeout_in_seconds != NULL)
    {
        std::string timeout(file_options.file_cache_timeout_in_seconds);
        str_options.fileCacheTimeoutInSeconds = stoi(timeout);
    }
    else
    {
        str_options.fileCacheTimeoutInSeconds = 120;
    }
    return 0;
}

int configure_tls()
{
    // For proper locking, instructing gcrypt to use pthreads 
    gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
    if(GNUTLS_E_SUCCESS != gnutls_global_init())
    {
        syslog (LOG_CRIT, "Unable to start blobfuse. GnuTLS initialization failed: errno = %d.\n", errno);
        fprintf(stderr, "GnuTLS initialization failed: errno = %d.\n", errno);
        return 1; 
    }
    return 0;
}

void configure_fuse(struct fuse_args *args)
{
    fuse_opt_add_arg(args, "-omax_read=131072");
    fuse_opt_add_arg(args, "-omax_write=131072");

    if (file_options.file_cache_timeout_in_seconds != NULL)
    {
        std::string timeout(file_options.file_cache_timeout_in_seconds);
        str_options.fileCacheTimeoutInSeconds = stoi(timeout);
    }
    else
    {
        str_options.fileCacheTimeoutInSeconds = 120;
    }

    // FUSE contains a feature where it automatically implements 'soft' delete if one process has a file open when another calls unlink().
    // This feature causes us a bunch of problems, so we use "-ohard_remove" to disable it, and track the needed 'soft delete' functionality on our own.
    fuse_opt_add_arg(args, "-ohard_remove");
    fuse_opt_add_arg(args, "-obig_writes");
    fuse_opt_add_arg(args, "-ofsname=blobfuse");
    fuse_opt_add_arg(args, "-okernel_cache");
    umask(0);
}

int initialize_blobfuse()
{
    if(0 != ensure_files_directory_exists_in_cache(prepend_mnt_path_string("/placeholder")))
    {
        syslog(LOG_CRIT, "Unable to start blobfuse.  Failed to create directory on cache directory: %s, errno = %d.\n", prepend_mnt_path_string("/placeholder").c_str(),  errno);
        fprintf(stderr, "Failed to create directory on cache directory: %s, errno = %d.\n", prepend_mnt_path_string("/placeholder").c_str(),  errno);
        return 1;
    }
    return 0;
}
