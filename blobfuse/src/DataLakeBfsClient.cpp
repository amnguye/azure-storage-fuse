#include <fstream>
#include <include/permissions.h>
#include "DataLakeBfsClient.h"
#include "list_paths_request.h"

///<summary>
/// Uploads contents of a file to a storage object(e.g. blob, file) to the Storage service
///</summary>
///TODO: params
///<returns>none</returns>
bool DataLakeBfsClient::AuthenticateStorage()
{
    // Authenticate the storage account
    switch (configurations.authType) {
        case KEY_AUTH:
            m_adls_client = authenticate_accountkey();
            break;
        case SAS_AUTH:
            m_adls_client = authenticate_sas();
            break;
        case MSI_AUTH:
            m_adls_client = authenticate_msi();
            break;
        default:
            return false;
            break;
    }

    if(m_adls_client != NULL)
    {
        //Authenticate the storage container by using a list call
        m_adls_client->list_paths_segmented(
                configurations.containerName,
                "/",
                1);
        if (errno != 0)
        {
            syslog(LOG_ERR,
                   "Unable to start blobfuse.  Failed to connect to the storage container. There might be something wrong about the storage config, please double check the storage account name, account key/sas token/OAuth access token and container name. errno = %d\n",
                   errno);
            return false;
        }
        return true;
    }
    return false;
}

std::shared_ptr<microsoft_azure::storage_adls::adls_client> DataLakeBfsClient::authenticate_accountkey()
{
    try
    {
        std::shared_ptr<storage_credential> cred;
        if (configurations.accountKey.length() > 0)
        {
            cred = std::make_shared<shared_key_credential>(configurations.accountName, configurations.accountKey);
        }
        else
        {
            syslog(LOG_ERR, "Empty account key. Failed to create blob client.");
            return NULL;
        }
        errno = 0;
        std::shared_ptr<storage_account> account = std::make_shared<storage_account>(
                configurations.accountName,
                cred,
                configurations.useHttps,
                configurations.blobEndpoint);
        return std::make_shared<microsoft_azure::storage_adls::adls_client>(
                account,
                max_concurrency_blob_wrapper,
                false); //If this applies to blobs in the future, we can use this as a feature to exit
                                // blobfuse if we run into anything unexpected instead of logging errors
    }
    catch(const std::exception &ex)
    {
        syslog(LOG_ERR, "Failed to create blob client.  ex.what() = %s.", ex.what());
        errno = unknown_error;
        return NULL;
    }
}
std::shared_ptr<microsoft_azure::storage_adls::adls_client> DataLakeBfsClient::authenticate_sas()
{
    try
    {
        std::shared_ptr<storage_credential> cred;
        if(configurations.sasToken.length() > 0)
        {
            cred = std::make_shared<shared_access_signature_credential>(configurations.sasToken);
        }
        else
        {
            syslog(LOG_ERR, "Empty account key. Failed to create blob client.");
            return NULL;
        }
        errno = 0;
        std::shared_ptr<storage_account> account = std::make_shared<storage_account>(
                configurations.accountName, cred,
                configurations.useHttps,
                configurations.blobEndpoint);
        return std::make_shared<microsoft_azure::storage_adls::adls_client>(
                account,
                max_concurrency_blob_wrapper,
                false); //If this applies to blobs in the future, we can use this as a feature to exit
                                // blobfuse if we run into anything unexpected instead of logging errors
    }
    catch(const std::exception &ex)
    {
        syslog(LOG_ERR, "Failed to create blob client.  ex.what() = %s.", ex.what());
        errno = unknown_error;
        return NULL;
    }
}
std::shared_ptr<microsoft_azure::storage_adls::adls_client> DataLakeBfsClient::authenticate_msi() {
    try {
        //1. get oauth token
        std::function<OAuthToken(std::shared_ptr<CurlEasyClient>)> MSICallback = SetUpMSICallback(
                configurations.clientId,
                configurations.objectId,
                configurations.resourceId,
                configurations.msiEndpoint);

        std::shared_ptr<OAuthTokenCredentialManager> tokenManager = GetTokenManagerInstance(MSICallback);

        if (!tokenManager->is_valid_connection()) {
            // todo: isolate definitions of errno's for this function so we can output something meaningful.
            errno = 1;
            return NULL;
        }

        //2. try to make blob client wrapper using oauth token
        errno = 0;
        std::shared_ptr<storage_credential> cred = std::make_shared<token_credential>();
        std::shared_ptr<storage_account> account = std::make_shared<storage_account>(
                configurations.accountName,
                cred,
                true, //use_https must be true to use oauth
                configurations.blobEndpoint);
        return std::make_shared<microsoft_azure::storage_adls::adls_client>(
                account,
                max_concurrency_blob_wrapper,
                false); //If this applies to blobs in the future, we can use this as a feature to exit
        // blobfuse if we run into anything unexpected instead of logging errors

    }
    catch (const std::exception &ex) {
        syslog(LOG_ERR, "Failed to create blob client.  ex.what() = %s. Please check your account name and ",
               ex.what());
        errno = unknown_error;
        return NULL;
    }
}
///<summary>
/// Creates a Directory
///</summary>
///<returns>none</returns>
bool DataLakeBfsClient::CreateDirectory(const std::string directoryPath)
{
    // We could call the block blob CreateDirectory instead but that would require making the metadata with hdi_isfolder
    // it's easier if we let the service do that for us
    errno = 0;
    m_adls_client->create_directory(configurations.containerName, directoryPath);
    if(errno != 0)
    {
        return false;
    }
    return true;
}
///<summary>
/// Deletes a Directory
///</summary>
///<returns>none</returns>
bool DataLakeBfsClient::DeleteDirectory(const std::string directoryPath)
{
    errno = 0;
    m_adls_client->delete_directory(configurations.containerName, directoryPath, false);
    if(errno != 0)
    {
        return false;
    }
    return true;
}

///<summary>
/// Helper function - Checks if the "directory" blob is empty
///</summary>
D_RETURN_CODE DataLakeBfsClient::IsDirectoryEmpty(std::string path)
{
    bool success = false;
    bool old_dir_blob_found = false;
    int failcount = 0;
    std::string continuation = "";
    do{
        errno = 0;
        microsoft_azure::storage_adls::list_paths_result pathsResult = m_adls_client->list_paths_segmented(
                configurations.containerName,
                path,
                false,
                std::string(),
                2);
        if(errno == 0)
        {
            success = true;
            failcount = 0;
            continuation = pathsResult.continuation_token;
            if (pathsResult.paths.size() > 1) {
                return D_NOTEMPTY;
            }
            if (pathsResult.paths.size() > 0)
            {
                // A blob of the previous folder ".." could still exist, that does not count as the directory still has
                // any existing blobs
                if ((!old_dir_blob_found) &&
                    (!pathsResult.paths[0].is_directory) &&
                    (pathsResult.paths[0].name.size() > former_directory_signifier.size()) &&
                    (0 == pathsResult.paths[0].name.compare(
                            pathsResult.paths[0].name.size() - former_directory_signifier.size(),
                             former_directory_signifier.size(),
                             former_directory_signifier)))
                {
                    old_dir_blob_found = true;
                } else
                    {
                    return D_NOTEMPTY;
                }
            }
        }
        else
        {
            success = false;
            failcount++;
        }
    }
    // If we get a continuation token, and the blob size on the first or so calls is still empty, the service could
    // actually have blobs in the container, but they just didn't send them in the request, but they have a
    // continuation token so it means they could have some.
    while ((continuation.size() > 0 || !success) && failcount < 20);

    if(!success)
    {
        return D_FAILED;
    }
    return old_dir_blob_found ? D_EMPTY : D_NOTEMPTY;
}

///<summary>
/// Renames a file/directory
///</summary>
///<returns></returns>
std::vector<std::string> DataLakeBfsClient::Rename(std::string sourcePath, std::string destinationPath)
{
    m_adls_client->move_file(
            configurations.containerName,
            sourcePath,
            configurations.containerName,
            destinationPath);
    //stub for now
    return std::vector<std::string>();
}

int DataLakeBfsClient::ChangeMode(const char *path, mode_t mode) {
    // TODO: Once ADLS works in blobfuse, verify that we don't need to get the access
    microsoft_azure::storage_adls::access_control accessControl;
    accessControl.acl = modeToString(mode);

    errno = 0;
    m_adls_client->set_file_access_control(configurations.containerName, path, accessControl);

    return errno;
}

BfsFileProperty DataLakeBfsClient::GetProperties(std::string pathName) {
    microsoft_azure::storage_adls::dfs_properties dfsprops =
            m_adls_client->get_dfs_path_properties(configurations.containerName, pathName);

    return BfsFileProperty(
            dfsprops.cache_control,
            dfsprops.content_disposition,
            dfsprops.content_encoding,
            dfsprops.content_language,
            dfsprops.content_md5,
            dfsprops.content_type,
            dfsprops.etag,
            "",
            dfsprops.metadata,
            dfsprops.last_modified,
            dfsprops.permissions,
            dfsprops.content_length
            );
}
