#include "blobfuse.h"
#include <sys/file.h>

int map_errno(int error)
{
    auto mapping = error_mapping.find(error);
    if (mapping == error_mapping.end())
    {
        syslog(LOG_INFO, "Failed to map storage error code %d to a proper errno.  Returning EIO = %d instead.\n", error, EIO);
        return EIO;
    }
    else
    {
        return mapping->second;
    }
}

std::string prepend_mnt_path_string(const std::string& path)
{
    std::string result;
    result.reserve(str_options.tmpPath.length() + 5 + path.length());
    return result.append(str_options.tmpPath).append("/root").append(path);
}

// Acquire shared lock utility function
int shared_lock_file(int flags, int fd)
{
    if((flags&O_NONBLOCK) == O_NONBLOCK)
    {
        if(0 != flock(fd, LOCK_SH|LOCK_NB))
        {
            int flockerrno = errno;
            if (flockerrno == EWOULDBLOCK)
            {
               AZS_DEBUGLOGV("Failure to acquire flock due to EWOULDBLOCK.  fd = %d.", fd);
            }
            else
            {
               syslog(LOG_ERR, "Failure to acquire flock for fd = %d.  errno = %d", fd, flockerrno);
            }
            close(fd);
            return 0 - flockerrno;
        }
    }
    else
    {
        if (0 != flock(fd, LOCK_SH))
        {
            int flockerrno = errno;
            syslog(LOG_ERR, "Failure to acquire flock for fd = %d.  errno = %d", fd, flockerrno);
            close(fd);
            return 0 - flockerrno;
        }
    }

    return 0;
}

bool is_directory_blob(unsigned long long size, std::vector<std::pair<std::string, std::string>> metadata)
{
    if (size == 0)
    {
        for (auto iter = metadata.begin(); iter != metadata.end(); ++iter)
        {
            if ((iter->first.compare("hdi_isfolder") == 0) && (iter->second.compare("true") == 0))
            {
                return true;
            }
        }
    }
    return false;
}

int ensure_files_directory_exists_in_cache(const std::string& file_path)
{
    char *pp;
    char *slash;
    int status;
    char *copypath = strdup(file_path.c_str());

    status = 0;
    errno = 0;
    pp = copypath;
    while (status == 0 && (slash = strchr(pp, '/')) != 0)
    {
        if (slash != pp)
        {
            *slash = '\0';
            AZS_DEBUGLOGV("Making cache directory %s.\n", copypath);
            struct stat st;
            if (stat(copypath, &st) != 0)
            {
                status = mkdir(copypath, str_options.default_permission);
            }

            // Ignore if some other thread was successful creating the path
	    if(errno == EEXIST)
            {
                status = 0;
                errno = 0;
            }

            *slash = '/';
        }
        pp = slash + 1;
    }
    free(copypath);
    return status;
}

int azs_getattr(const char *path, struct stat *stbuf)
{
    AZS_DEBUGLOGV("azs_getattr called with path = %s\n", path);
    // If we're at the root, we know it's a directory
    if (strlen(path) == 1)
    {
        stbuf->st_mode = S_IFDIR | str_options.default_permission; // TODO: proper access control.
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 2; // Directories should have a hard-link count of 2 + (# child directories).  We don't have that count, though, so we just use 2 for now.  TODO: Evaluate if we could keep this accurate or not.
        stbuf->st_size = 4096;
        stbuf->st_mtime = time(NULL);
        return 0;
    }

    // Ensure that we don't get attributes while the file is in an intermediate state.
    auto fmutex = file_lock_map::get_instance()->get_mutex(path);
    std::lock_guard<std::mutex> lock(*fmutex);

    // Check and see if the file/directory exists locally (because it's being buffered.)  If so, skip the call to Storage.
    std::string pathString(path);
    std::string mntPathString = prepend_mnt_path_string(pathString);

    int res;
    int acc = access(mntPathString.c_str(), F_OK);
    if (acc != -1 )
    {
        AZS_DEBUGLOGV("Accessing mntPath = %s for get_attr succeeded; object is in the local cache.\n", mntPathString.c_str());
        //(void) fi;
        res = lstat(mntPathString.c_str(), stbuf);
        if (res == -1)
        {
            int lstaterrno = errno;
            syslog(LOG_ERR, "lstat on file %s in local cache during get_attr failed with errno = %d.\n", mntPathString.c_str(), lstaterrno);
            return -lstaterrno;
        }
        else
        {
            AZS_DEBUGLOGV("lstat on file %s in local cache succeeded.\n", mntPathString.c_str());
            return 0;
        }
    }
    else
    {
        AZS_DEBUGLOGV("Object %s is not in the local cache during get_attr.\n", mntPathString.c_str());
    }

    // It's not in the local cache.  Check to see if it's a blob on the service:
    std::string blobNameStr(&(path[1]));
    errno = 0;
    BfsFileProperty blob_property = storage_client->GetProperties(blobNameStr);

    if ((errno == 0) && blob_property.isValid())
    {
        if (is_directory_blob(blob_property.size(), blob_property.m_metadata))
        {
            AZS_DEBUGLOGV("Blob %s, representing a directory, found during get_attr.\n", path);
            stbuf->st_mode = S_IFDIR | str_options.default_permission;
            // If st_nlink = 2, means directory is empty.
            // Directory size will affect behaviour for mv, rmdir, cp etc.
            stbuf->st_uid = fuse_get_context()->uid;
            stbuf->st_gid = fuse_get_context()->gid;
            stbuf->st_nlink = storage_client->IsDirectory(blobNameStr.c_str()) == D_EMPTY ? 2 : 3;
            stbuf->st_size = 4096;
            return 0;
        }

        AZS_DEBUGLOGV("Blob %s, representing a file, found during get_attr.\n", path);
        stbuf->st_mode = S_IFREG | str_options.default_permission; // Regular file (not a directory)
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_mtime = blob_property.last_modified();
        stbuf->st_nlink = 1;
        stbuf->st_size = blob_property.size();
        return 0;
    }
    else if (errno == 0 && !blob_property.isValid())
    {
        // Check to see if it's a directory, instead of a file

        errno = 0;
        int dirSize = storage_client->IsDirectory(blobNameStr.c_str());
        if (errno != 0)
        {
            int storage_errno = errno;
            syslog(LOG_ERR, "Failure when attempting to determine if directory %s exists on the service.  errno = %d.\n", blobNameStr.c_str(), storage_errno);
            return 0 - map_errno(storage_errno);
        }
        if (dirSize != D_NOTEXIST)
        {
            AZS_DEBUGLOGV("Directory %s found on the service.\n", blobNameStr.c_str());
            stbuf->st_mode = S_IFDIR | str_options.default_permission;
            // If st_nlink = 2, means direcotry is empty.
            // Directory size will affect behaviour for mv, rmdir, cp etc.
            stbuf->st_uid = fuse_get_context()->uid;
            stbuf->st_gid = fuse_get_context()->gid;
            stbuf->st_nlink = dirSize == D_EMPTY ? 2 : 3;
            stbuf->st_size = 4096;
            return 0;
        }
        else
        {
            AZS_DEBUGLOGV("Entity %s does not exist.  Returning ENOENT (%d) from get_attr.\n", path, ENOENT);
            return -(ENOENT);
        }
    }
    else
    {
        int storage_errno = errno;
        syslog(LOG_ERR, "Failure when attempting to determine if %s exists on the service.  errno = %d.\n", blobNameStr.c_str(), storage_errno);
        return 0 - map_errno(storage_errno);
    }
}

// Helper method for FTW to remove an entire directory & it's contents.
int rm(const char *fpath, const struct stat * /*sb*/, int tflag, struct FTW * /*ftwbuf*/)
{
    if (tflag == FTW_DP)
    {
        errno = 0;
        int ret = rmdir(fpath);
        return ret;
    }
    else
    {
        errno = 0;
        int ret = unlink(fpath);
        return ret;
    }
}

// Delete the entire contents of tmpPath.
void azs_destroy(void * /*private_data*/)
{
    AZS_DEBUGLOG("azs_destroy called.\n");
    std::string rootPath(str_options.tmpPath + "/root");

    errno = 0;
    // FTW_DEPTH instructs FTW to do a post-order traversal (children of a directory before the actual directory.)
    nftw(rootPath.c_str(), rm, 20, FTW_DEPTH); 
}


// Not yet implemented section:
int azs_access(const char * /*path*/, int /*mask*/)
{
    return 0;  // permit all access
}

int azs_readlink(const char * /*path*/, char * /*buf*/, size_t /*size*/)
{
    return -EINVAL; // not a symlink
}

int azs_fsync(const char * /*path*/, int /*isdatasync*/, struct fuse_file_info * /*fi*/)
{
    return 0; // Skip for now
}

int azs_chown(const char * /*path*/, uid_t /*uid*/, gid_t /*gid*/)
{
    //TODO: Implement
//    return -ENOSYS;
    return 0;
}

int azs_chmod(const char * /*path*/, mode_t /*mode*/)
{
    //TODO: Implement
//    return -ENOSYS;
    return 0;

}

//#ifdef HAVE_UTIMENSAT
int azs_utimens(const char * /*path*/, const struct timespec [2] /*ts[2]*/)
{
    //TODO: Implement
//    return -ENOSYS;
    return 0;
}
//  #endif

int azs_rename_directory(const char *src, const char *dst)
{
    AZS_DEBUGLOGV("azs_rename_directory called with src = %s, dst = %s.\n", src, dst);
    std::string srcPathStr(src);
    std::string dstPathStr(dst);

    // Rename the directory blob, if it exists.
    errno = 0;
    if (storage_client->Exists(srcPathStr))
    {
        azs_rename_single_file(src, dst);
    }
    if (errno != 0)
    {
        if ((errno != 404) && (errno != ENOENT))
        {
            return 0 - map_errno(errno); // Failure in fetching properties - errno set by blob_exists
        }
    }

    if (srcPathStr.size() > 1)
    {
        srcPathStr.push_back('/');
    }
    if (dstPathStr.size() > 1)
    {
        dstPathStr.push_back('/');
    }
    std::vector<std::string> local_list_results;

    // Rename all files and directories that exist in the local cache.
    ensure_files_directory_exists_in_cache(prepend_mnt_path_string(dstPathStr + "placeholder"));
    std::string mntPathString = prepend_mnt_path_string(srcPathStr);
    DIR *dir_stream = opendir(mntPathString.c_str());
    if (dir_stream != NULL)
    {
        struct dirent* dir_ent = readdir(dir_stream);
        while (dir_ent != NULL)
        {
            if (dir_ent->d_name[0] != '.')
            {
                int nameLen = strlen(dir_ent->d_name);
                char *newSrc = (char *)malloc(sizeof(char) * (srcPathStr.size() + nameLen + 1));
                memcpy(newSrc, srcPathStr.c_str(), srcPathStr.size());
                memcpy(&(newSrc[srcPathStr.size()]), dir_ent->d_name, nameLen);
                newSrc[srcPathStr.size() + nameLen] = '\0';

                char *newDst = (char *)malloc(sizeof(char) * (dstPathStr.size() + nameLen + 1));
                memcpy(newDst, dstPathStr.c_str(), dstPathStr.size());
                memcpy(&(newDst[dstPathStr.size()]), dir_ent->d_name, nameLen);
                newDst[dstPathStr.size() + nameLen] = '\0';

                AZS_DEBUGLOGV("Local object found - about to rename %s to %s.\n", newSrc, newDst);
                if (dir_ent->d_type == DT_DIR)
                {
                    azs_rename_directory(newSrc, newDst);
                }
                else
                {
                    azs_rename_single_file(newSrc, newDst);
                }

                free(newSrc);
                free(newDst);

                std::string dir_str(dir_ent->d_name);
                local_list_results.push_back(dir_str);
            }

            dir_ent = readdir(dir_stream);
        }

        closedir(dir_stream);
    }

    // Rename all files & directories that don't exist in the local cache.
    errno = 0;
    std::vector<std::pair<std::vector<list_hierarchical_item>, bool>> listResults = storage_client->ListAllItemsHierarchical("/", srcPathStr.substr(1));
    if (errno != 0)
    {
        int storage_errno = errno;
        syslog(LOG_ERR, "list blobs operation failed during attempt to rename directory %s to %s.  errno = %d.\n", src, dst, storage_errno);
        return 0 - map_errno(storage_errno);
    }

    AZS_DEBUGLOGV("Total of %s result lists found from list_blobs call during rename operation\n.", to_str(listResults.size()).c_str());
    for (size_t result_lists_index = 0; result_lists_index < listResults.size(); result_lists_index++)
    {
        int start = listResults[result_lists_index].second ? 1 : 0;
        for (size_t i = start; i < listResults[result_lists_index].first.size(); i++)
        {
            // We need to parse out just the trailing part of the path name.
            int len = listResults[result_lists_index].first[i].name.size();
            if (len > 0)
            {
                std::string prev_token_str;
                if (listResults[result_lists_index].first[i].name.back() == '/')
                {
                    prev_token_str = listResults[result_lists_index].first[i].name.substr(srcPathStr.size() - 1, listResults[result_lists_index].first[i].name.size() - srcPathStr.size());
                }
                else
                {
                    prev_token_str = listResults[result_lists_index].first[i].name.substr(srcPathStr.size() - 1);
                }

                // TODO: order or hash the list to improve perf
                if ((prev_token_str.size() > 0) && (std::find(local_list_results.begin(), local_list_results.end(), prev_token_str) == local_list_results.end()))
                {
                    int nameLen = prev_token_str.size();
                    char *newSrc = (char *)malloc(sizeof(char) * (srcPathStr.size() + nameLen + 1));
                    memcpy(newSrc, srcPathStr.c_str(), srcPathStr.size());
                    memcpy(&(newSrc[srcPathStr.size()]), prev_token_str.c_str(), nameLen);
                    newSrc[srcPathStr.size() + nameLen] = '\0';

                    char *newDst = (char *)malloc(sizeof(char) * (dstPathStr.size() + nameLen + 1));
                    memcpy(newDst, dstPathStr.c_str(), dstPathStr.size());
                    memcpy(&(newDst[dstPathStr.size()]), prev_token_str.c_str(), nameLen);
                    newDst[dstPathStr.size() + nameLen] = '\0';

                    AZS_DEBUGLOGV("Object found on the service - about to rename %s to %s.\n", newSrc, newDst);
                    if (listResults[result_lists_index].first[i].is_directory)
                    {
                        azs_rename_directory(newSrc, newDst);
                    }
                    else
                    {
                        azs_rename_single_file(newSrc, newDst);
                    }

                    free(newSrc);
                    free(newDst);
                }
            }
        }
    }
    azs_rmdir(src);
    return 0;
}

// TODO: Fix bug where the files and directories in the source in the file cache are not deleted.
// TODO: Fix bugs where the a file has been created but not yet uploaded.
// TODO: Fix the bug where this fails for multi-level dirrectories.
// TODO: If/when we upgrade to FUSE 3.0, we will need to worry about the additional possible flags (RENAME_EXCHANGE and RENAME_NOREPLACE)
int azs_rename(const char *src, const char *dst)
{
    AZS_DEBUGLOGV("azs_rename called with src = %s, dst = %s.\n", src, dst);

    struct stat statbuf;
    errno = 0;
    int getattrret = azs_getattr(src, &statbuf);
    if (getattrret != 0)
    {
        return getattrret;
    }
    if ((statbuf.st_mode & S_IFDIR) == S_IFDIR)
    {
        azs_rename_directory(src, dst);
    }
    else
    {
        azs_rename_single_file(src, dst);
    }

    return 0;
}


int azs_setxattr(const char * /*path*/, const char * /*name*/, const char * /*value*/, size_t /*size*/, int /*flags*/)
{
    return -ENOSYS;
}
int azs_getxattr(const char * /*path*/, const char * /*name*/, char * /*value*/, size_t /*size*/)
{
    return -ENOSYS;
}
int azs_listxattr(const char * /*path*/, char * /*list*/, size_t /*size*/)
{
    return -ENOSYS;
}
int azs_removexattr(const char * /*path*/, const char * /*name*/)
{
    return -ENOSYS;
}
