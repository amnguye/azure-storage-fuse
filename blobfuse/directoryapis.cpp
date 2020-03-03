#include "blobfuse.h"
#include <sys/file.h>

// TODO: Bug in azs_mkdir, should fail if the directory already exists.
int azs_mkdir(const char *path, mode_t)
{
    AZS_DEBUGLOGV("mkdir called with path = %s\n", path);

    std::string pathstr(path);

    errno = 0;
    storage_client->CreateDirectory(pathstr.substr(1).c_str());
    if (errno != 0)
    {
        int storage_errno = errno;
        syslog(LOG_ERR, "Failed to create directory for path: %s.  errno = %d.\n", path, storage_errno);
        return 0 - map_errno(errno);
    }
    else
    {
        syslog(LOG_INFO, "Successfully created directory path: %s. ", path);
    }
    return 0;
}

/**
 * Read the contents of a directory.  For each entry to add, call the filler function with the input buffer,
 * the name of the entry, and additional data about the entry.  TODO: Keep the data (somehow) for latter getattr calls.
 *
 * @param  path   Path to the directory to read.
 * @param  buf    Buffer to pass into the filler function.  Not otherwise used in this function.
 * @param  filler Function to call to add directories and files as they are discovered.
 * @param  offset Not used
 * @param  fi     File info about the directory to be read.
 * @param  flags  Not used.  TODO: Consider prefetching on FUSE_READDIR_PLUS.
 * @return        TODO: error codes.
 */
int azs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t, struct fuse_file_info * fi)
{
    AZS_DEBUGLOGV("azs_readdir called with path = %s, fi = %d\n", path, (int)fi->fh);

    std::string pathStr;
    if(path != NULL)
    {
        pathStr = (std::string)path;
    }
    else
    {
        return -ENOENT;
    }
    if (pathStr.size() > 1)
    {
        pathStr.push_back('/');
    }

    std::vector<std::string> local_list_results;

    // Scan for any files that exist in the local cache.
    // It is possible that there are files in the cache that aren't on the service - if a file has been opened but not yet uploaded, for example.
    // TODO: Optimize the scenario where the file is open for read/write, but no actual writing occurs, to not upload the blob.
    //struct fhwrapper *fhwrap = (fhwrapper*) fi->fh;
    //int folder_handle = fhwrap->fh;

    std::string mntPathString = prepend_mnt_path_string(pathStr);
    DIR *dir_stream = opendir(mntPathString.c_str());
    if (dir_stream != NULL)
    {
        AZS_DEBUGLOGV("Reading contents of local cache directory %s.\n", mntPathString.c_str());
        struct dirent* dir_ent = readdir(dir_stream);
        while (dir_ent != NULL)
        {
            if (dir_ent->d_name[0] != '.')
            {
                if (dir_ent->d_type == DT_DIR)
                {
                    struct stat stbuf;
                    stbuf.st_mode = S_IFDIR | str_options.defaultPermission;
                    stbuf.st_uid = fuse_get_context()->uid;
                    stbuf.st_gid = fuse_get_context()->gid;
                    stbuf.st_nlink = 2;
                    stbuf.st_size = 4096;
                    filler(buf, dir_ent->d_name, &stbuf, 0);
                    AZS_DEBUGLOGV("Subdirectory %s found in local cache directory %s during readdir operation.\n", dir_ent->d_name, mntPathString.c_str());
                }
                else
                {
                    struct stat buffer;
                    stat((mntPathString + dir_ent->d_name).c_str(), &buffer);

                    struct stat stbuf;
                    stbuf.st_mode = S_IFREG | str_options.defaultPermission; // Regular file (not a directory)
                    stbuf.st_uid = fuse_get_context()->uid;
                    stbuf.st_gid = fuse_get_context()->gid;
                    stbuf.st_nlink = 1;
                    stbuf.st_size = buffer.st_size;
                    filler(buf, dir_ent->d_name, &stbuf, 0); // TODO: Add stat information.  Consider FUSE_FILL_DIR_PLUS.
                    AZS_DEBUGLOGV("File %s found in local cache directory %s during readdir operation.\n", dir_ent->d_name, mntPathString.c_str());
                }

                std::string dir_str(dir_ent->d_name);
                local_list_results.push_back(dir_str);
            }

            dir_ent = readdir(dir_stream);
        }
        closedir(dir_stream);
    }
    else
    {
        AZS_DEBUGLOGV("Directory %s not found in file cache during readdir operation for %s.\n", mntPathString.c_str(), path);
    }

    errno = 0;
    std::vector<std::pair<std::vector<list_hierarchical_item>, bool>> listResults = storage_client->ListAllItemsHierarchical("/", pathStr.substr(1));
    if (errno != 0)
    {
        int storage_errno = errno;
        syslog(LOG_ERR, "Failed to list blobs under directory %s on the service during readdir operation.  errno = %d.\n", mntPathString.c_str(), storage_errno);
        return 0 - map_errno(storage_errno);
    }
    else
    {
        AZS_DEBUGLOGV("Reading blobs of directory %s on the service.  Total blob lists found = %s.\n", pathStr.c_str()+1, to_str(listResults.size()).c_str());
    }

    // Fill the blobfuse current and parent directories
    struct stat stcurrentbuf, stparentbuf;
    stcurrentbuf.st_mode = S_IFDIR | str_options.defaultPermission;
    stparentbuf.st_mode = S_IFDIR;

    filler(buf, ".", &stcurrentbuf, 0);
    filler(buf, "..", &stparentbuf, 0);

    // Enumerating segments of list_blobs response
    for (size_t result_lists_index = 0; result_lists_index < listResults.size(); result_lists_index++)
    {
        // Check to see if the first list_blobs__hierarchical_item can be skipped to avoid duplication
        int start = listResults[result_lists_index].second ? 1 : 0;
        for (size_t i = start; i < listResults[result_lists_index].first.size(); i++)
        {
            int fillerResult;
            // We need to parse out just the trailing part of the path name.
            int len = listResults[result_lists_index].first[i].name.size();
            if (len > 0)
            {
                std::string prev_token_str;
                if (listResults[result_lists_index].first[i].name.back() == '/')
                {
                    prev_token_str = listResults[result_lists_index].first[i].name.substr(pathStr.size() - 1, listResults[result_lists_index].first[i].name.size() - pathStr.size());
                }
                else
                {
                    prev_token_str = listResults[result_lists_index].first[i].name.substr(pathStr.size() - 1);
                }

                // Any files that exist both on the service and in the local cache will be in both lists, we need to de-dup them.
                // TODO: order or hash the list to improve perf
                if (std::find(local_list_results.begin(), local_list_results.end(), prev_token_str) == local_list_results.end())
                {
                    if (!listResults[result_lists_index].first[i].is_directory && !is_directory_blob(listResults[result_lists_index].first[i].content_length, listResults[result_lists_index].first[i].metadata))
                    {
                        if ((prev_token_str.size() > 0) && (strcmp(prev_token_str.c_str(), former_directory_signifier.c_str()) != 0))
                        {
                            struct stat stbuf;
                            stbuf.st_mode = S_IFREG | str_options.defaultPermission; // Regular file (not a directory)
                            stbuf.st_uid = fuse_get_context()->uid;
                            stbuf.st_gid = fuse_get_context()->gid;
                            stbuf.st_nlink = 1;
                            stbuf.st_size = listResults[result_lists_index].first[i].content_length;
                            fillerResult = filler(buf, prev_token_str.c_str(), &stbuf, 0); // TODO: Add stat information.  Consider FUSE_FILL_DIR_PLUS.
                            AZS_DEBUGLOGV("Blob %s found in directory %s on the service during readdir operation.  Adding to readdir list; fillerResult = %d.\n", prev_token_str.c_str(), pathStr.c_str()+1, fillerResult);
                        }
                    }
                    else
                    {
                        if (prev_token_str.size() > 0)
                        {

                            // Avoid duplicate directories - this avoids duplicate entries of legacy WASB and HNS directories
                   	        local_list_results.push_back(prev_token_str);

                            struct stat stbuf;
                            stbuf.st_mode = S_IFDIR | str_options.defaultPermission;
                            stbuf.st_uid = fuse_get_context()->uid;
                            stbuf.st_gid = fuse_get_context()->gid;
                            stbuf.st_nlink = 2;
                            fillerResult = filler(buf, prev_token_str.c_str(), &stbuf, 0);
                            AZS_DEBUGLOGV("Blob directory %s found in directory %s on the service during readdir operation.  Adding to readdir list; fillerResult = %d.\n", prev_token_str.c_str(), pathStr.c_str()+1, fillerResult);
                        }
                    }
                }
                else
                {
                    AZS_DEBUGLOGV("Skipping adding blob %s to readdir results because it was already added from the local cache.\n", prev_token_str.c_str());
                }
            }
        }
    }
    return 0;
}

/**
 * Opens a folder for reading
 * TODO: talk about implementation
 *
 * @param path The path to the folder to open
 * @parm fi File info. Contains the flags to use in opendir()
 * @return error code (0 if success)
 */
int azs_opendir(const char * path, struct fuse_file_info* fi)
{
    syslog (LOG_DEBUG, "azs_opendir called with path = %s, fi->flags = %X.\n", path, fi->flags);
    std::string pathString(path);
    const char * mntPath;
    std::string mntPathString = prepend_mnt_path_string(pathString);
    mntPath = mntPathString.c_str();

    // Here, we lock the directory path using the mutex.  This ensures that multiple threads aren't trying alter the directory
    // We cannot use "flock" to prevent against this, because a) the directory might not yet exist, and b) flock locks do not persist across directory delete / recreate operations, and file renames.
    auto fmutex = file_lock_map::get_instance()->get_mutex(path);
    std::lock_guard<std::mutex> lock(*fmutex);

    // If the directory does not exist in the cache, or the version in the cache is too old, we need to download / refresh the directory from the service.
    // If the directory attributes hasn't been modified
    // We only want to refresh if enough time has passed that both are more than cache_timeout seconds ago.
    struct stat buf;
    int statret = stat(mntPath, &buf);
    time_t now = time(NULL);
    if ((statret != 0) ||
        (((now - buf.st_mtime) > str_options.fileCacheTimeoutInSeconds) &&
         ((now - buf.st_ctime) > str_options.fileCacheTimeoutInSeconds)))
    {
        bool skipCacheUpdate = false;
        if (statret == 0) // File exists
        {
            // Here, we take an exclusive flock lock on the directory in the cache.
            // This ensures that there are no existing open handles to the cached directory.
            // This operation cannot deadlock with the mutex acquired above, because we acquire the lock in non-blocking mode.

            errno = 0;
            int fd = open(mntPath, O_DIRECTORY);
            if (fd == -1)
            {
                syslog (LOG_DEBUG, "Path %s, is not a directory, errno: %d", path, errno);
                return -errno;
            }

            errno = 0;
            int flockres = flock(fd, LOCK_SH | LOCK_NB);
            if (flockres != 0)
            {
                if (errno == EWOULDBLOCK)
                {
                    // Someone else holds the lock.  In this case, we will postpone updating the cache until the next time open() is called.
                    // TODO: examine the possibility that we can never acquire the lock and refresh the cache.
                    skipCacheUpdate = true;
                }
                else
                {
                    // Failed to acquire the lock for some other reason.  We close the open fd, and fail.
                    int flockerrno = errno;
                    syslog(LOG_ERR, "Failed to open %s; unable to acquire flock on directory %s in cache directory.  Errno = %d", path, mntPath, flockerrno);
                    close(fd);
                    return -flockerrno;
                }
            }
            flock(fd, LOCK_UN | LOCK_NB);
            close(fd);
            // We now know that there are no other open file/directory handles to the file.  We're safe to continue with the cache update.
        }

        if (!skipCacheUpdate)
        {
            if(0 != ensure_files_directory_exists_in_cache(mntPathString))
            {
                syslog(LOG_ERR, "Failed to create file or directory on cache directory: %s, errno = %d.\n", mntPathString.c_str(),  errno);
                return -1;
            }

            errno = 0;
            BfsFileProperty directory_properties = storage_client->GetProperties(pathString.substr(1));
            if(directory_properties.isValid())
            {
                //makes directory and sets permissions
                mode_t perms = directory_properties.m_file_mode == 0 ? (S_IFDIR | str_options.defaultPermission) : directory_properties.m_file_mode;
                struct stat info;
                int stat_res = stat(path, &info);
                if( stat_res != ENOENT )
                {
                    //no cache directory has been made
                    syslog(LOG_INFO, "Creating directory path: %s, in cache.\n", mntPathString.c_str());
                    //TODO: Look into updating the gc cache to eliminate folders when it expires
                    mkdir(mntPathString.c_str(),perms);
                }
                else if( ( stat_res != 0) && (info.st_mode & S_IFDIR ))  // S_ISDIR() doesn't exist on my windows
                {
                    //directory already exists, update if necessary
                    syslog(LOG_DEBUG, "Directory exists in cache. Updating properties of %s.\n", mntPathString.c_str());
                    chmod(path, perms);
                }
                else
                {
                    // directory is unable to access
                    syslog(LOG_ERR, "Cannot access directory: %s, errno = %d.\n", mntPathString.c_str(),  errno);
                    return errno;
                }
                errno = 0;
            }
            else
            {
                //directory does not exist or failed to get properties
                syslog(LOG_ERR, "Failed to retrieve Directory properties. Path name: %s, storage errno = %d.\n", pathString.c_str(), errno);
                return ENOENT;
            }
            time_t last_modified = {};
            if (errno != 0)
            {
                int storage_errno = errno;
                syslog(LOG_ERR, "Failed to download directory into cache.  Path name: %s, storage errno = %d.\n", pathString.c_str()+1,  errno);

                remove(mntPath);
                return 0 - map_errno(storage_errno);
            }
            else
            {
                syslog(LOG_INFO, "Successfully downloaded directory %s into file cache as %s.\n", pathString.c_str()+1, mntPathString.c_str());
            }

            // preserve the last modified time
            struct utimbuf new_time;
            new_time.modtime = last_modified;
            new_time.actime = 0;
            utime(mntPathString.c_str(), &new_time);
        }
    }

    errno = 0;
    int res;

    // Open a file handle to the file in the cache.
    // This will be stored in 'fi', and used for later read/write operations.
    res = open(mntPath, fi->flags);

    if (res == -1)
    {
        syslog(LOG_ERR, "Failed to open file %s in file cache.  errno = %d.", mntPathString.c_str(),  errno);
        return -errno;
    }
    AZS_DEBUGLOGV("Opening %s gives fh = %d, errno = %d", mntPath, res, errno);

    // At this point, the directory exists in the cache and we have an open file/directory handle to it.
    // We now attempt to acquire the flock lock in shared mode.
    int lock_result = shared_lock_file(fi->flags, res);
    if(lock_result != 0)
    {
        syslog(LOG_ERR, "Failed to acquire flock on directory %s in file cache.  errno = %d.", mntPathString.c_str(), lock_result);
        return lock_result;
    }

    // TODO: Actual access control
    fchmod(res, str_options.defaultPermission);

    // Store the open file handle, and whether or not the file should be uploaded on close().
    // TODO: Optimize the scenario where the file is open for read/write, but no actual writing occurs, to not upload the blob.
    struct fhwrapper *fhwrap = new fhwrapper(res, (((fi->flags & O_WRONLY) == O_WRONLY) || ((fi->flags & O_RDWR) == O_RDWR)));
    fi->fh = (long unsigned int)fhwrap; // Store the file handle for later use.

    AZS_DEBUGLOGV("Returning success from azs_opendir, directory = %s\n", path);
    return 0;
    /*
    AZS_DEBUGLOGV("azs_opendir called with path = %s\n", path);
    std::string pathstr(path);
    if(pathstr.size() == 1)
    {
        //root directory
        return 0;
    }
    // Scan for the directory that if it exists in the cache already
    // It is possible that there are directories in the cache that aren't on the service - if a directory has been opened but not yet uploaded, for example.
    std::string mntPathString = prepend_mnt_path_string(pathstr);
    DIR *dir_stream = opendir(mntPathString.c_str());
    if (dir_stream != NULL)
    {
        return 0;
    }
    else
    {
        AZS_DEBUGLOGV("Directory %s not found in file cache during opendir operation for %s.\n", mntPathString.c_str(), path);
    }
    if(storage_client->Exists(path))
    {
        return 0;
    }
    return ENOENT;
     */
}
/*
 * Removes empty directories
 */
int azs_rmdir(const char *path)
{
    AZS_DEBUGLOGV("azs_rmdir called with path = %s\n", path);

    std::string pathString(path);
    const char * mntPath;
    std::string mntPathString = prepend_mnt_path_string(pathString);
    mntPath = mntPathString.c_str();

    AZS_DEBUGLOGV("Attempting to delete local cache directory %s.\n", mntPath);
    remove(mntPath); // This will fail if the cache is not empty, which is fine, as in this case it will also fail later, after the server-side check.

    if(!storage_client->DeleteDirectory(pathString.substr(1)))
    {
        return -errno;
    }
    return 0;
}

/*
 * Releases handle for directories
 */
int azs_releasedir(const char* path, struct fuse_file_info *fi)
{
    AZS_DEBUGLOGV("azs_releasedir called with path = %s.\n", path);
    // Unlock the directory
    // Note that this will release the shared lock acquired in the corresponding open() call (the one that gave us this file descriptor, in the fuse_file_info).
    // It will not release any locks acquired from other calls to open(), in this process or in others.
    // If the file handle is invalid, this will fail with EBADF, which is not an issue here.
    flock(((struct fhwrapper *)fi->fh)->fh, LOCK_UN);

    // Close the file handle.
    // This must be done, even if the file no longer exists, otherwise we're leaking file handles.
    close(((struct fhwrapper *)fi->fh)->fh);

// TODO: Make this method resiliant to renames of the file (same way flush() is)

    delete (struct fhwrapper *)fi->fh;
    return 0;
}

int azs_statfs(const char *path, struct statvfs *stbuf)
{
    AZS_DEBUGLOGV("azs_statfs called with path = %s.\n", path);
    std::string pathString(path);

    struct stat statbuf;
    int getattrret = azs_getattr(path, &statbuf);
    if (getattrret != 0)
    {
        return getattrret;
    }

    // return tmp path stats
    errno = 0;
    int res = statvfs(str_options.tmpPath.c_str(), stbuf);
    if (res == -1)
        return -errno;

    return 0;
}
