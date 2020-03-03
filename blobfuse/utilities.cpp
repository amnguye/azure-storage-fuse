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
                status = mkdir(copypath, str_options.defaultPermission);
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
        stbuf->st_mode = S_IFDIR | str_options.defaultPermission; // TODO: proper access control.
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 2; // Directories should have a hard-link count of 2 + (# child directories).  We don't have that count, though, so we just use 2 for now.  TODO: Evaluate if we could keep this accurate or not.
        stbuf->st_size = 4096;
        stbuf->st_mtime = time(NULL);
        return 0;
    }

    // Ensure that we don't get attributes while the file is in an intermediate state.
    std::shared_ptr<std::mutex> fmutex = file_lock_map::get_instance()->get_mutex(path);
    std::lock_guard<std::mutex> lock(*fmutex);

    // Check and see if the file/directory exists locally (because it's being buffered.)  If so, skip the call to Storage.
    std::string pathString(path);
    std::string mntPathString = prepend_mnt_path_string(pathString);

    int res;
    int acc = access(mntPathString.c_str(), F_OK);
    if (acc != -1)
    {
        AZS_DEBUGLOGV("Accessing mntPath = %s for getattr succeeded; object is in the local cache.\n", mntPathString.c_str());
        //(void) fi;
        res = lstat(mntPathString.c_str(), stbuf);
        if (res == -1)
        {
            int lstaterrno = errno;
            syslog(LOG_ERR, "lstat on file %s in local cache during getattr failed with errno = %d.\n", mntPathString.c_str(), lstaterrno);
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
        AZS_DEBUGLOGV("Object %s is not in the local cache during getattr.\n", mntPathString.c_str());
    }

    // It's not in the local cache.  Check to see if it's a blob on the service:
    std::string blobNameStr(&(path[1]));
    errno = 0;
    BfsFileProperty blob_property = storage_client->GetProperties(blobNameStr);
    mode_t perms = blob_property.m_file_mode == 0 ?  str_options.defaultPermission : blob_property.m_file_mode;

    if ((errno == 0) && blob_property.isValid())
    {
        if (is_directory_blob(blob_property.size(), blob_property.m_metadata))
        {
            AZS_DEBUGLOGV("Blob %s, representing a directory, found during get_attr.\n", path);
            stbuf->st_mode = S_IFDIR | perms;
            // If st_nlink = 2, means directory is empty.
            // Directory size will affect behaviour for mv, rmdir, cp etc.
            stbuf->st_uid = fuse_get_context()->uid;
            stbuf->st_gid = fuse_get_context()->gid;
            stbuf->st_nlink = storage_client->IsDirectoryEmpty(blobNameStr.c_str()) == D_EMPTY ? 2 : 3;
            stbuf->st_size = 4096;
            return 0;
        }

        AZS_DEBUGLOGV("Blob %s, representing a file, found during get_attr.\n", path);
        stbuf->st_mode = S_IFREG | perms; // Regular file (not a directory)
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
        int dirSize = is_directory_blob(blob_property.size(), blob_property.m_metadata);
        if (errno != 0)
        {
            int storage_errno = errno;
            syslog(LOG_ERR, "Failure when attempting to determine if directory %s exists on the service.  errno = %d.\n", blobNameStr.c_str(), storage_errno);
            return 0 - map_errno(storage_errno);
        }
        if (dirSize != D_NOTEXIST)
        {
            AZS_DEBUGLOGV("Directory %s found on the service.\n", blobNameStr.c_str());
            stbuf->st_mode = S_IFDIR | str_options.defaultPermission;
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
        if(errno == 404)
        {
            // The file does not currently exist on the service or in the cache
            // If the command they are calling is just checking for existence, fuse will call the next operation
            // dependent on this error number. If the command cannot continue without the existence it will print out
            // the correct error to the user.
            syslog(LOG_WARNING, "File does not currently exist on the storage or cache");
            return -(ENOENT);
        }
        // If we received a different error, then let's fail with that error
        int storage_errno = errno;
        syslog(LOG_ERR, "Failure when attempting to determine if %s exists on the service.  errno = %d.\n", blobNameStr.c_str(), storage_errno);
        return 0 - map_errno(storage_errno);
    }
}

// After create is called, sometimes fgetattr gets called. This is also called when invoked by a user program

int azs_fgetattr(const char *path, struct stat* stbuf, fuse_file_info* /*fi*/)
{
    AZS_DEBUGLOGV("azs_fgetattr called with path = %s\n", path);

    // If we're at the root, we know it's a directory
    if (strlen(path) == 1)
    {
        stbuf->st_mode = S_IFDIR | str_options.defaultPermission; // TODO: proper access control.
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 2; // Directories should have a hard-link count of 2 + (# child directories).  We don't have that count, though, so we just use 2 for now.  TODO: Evaluate if we could keep this accurate or not.
        stbuf->st_size = 4096;
        stbuf->st_mtime = time(NULL);
        return 0;
    }

    // Ensure that we don't get attributes while the file is in an intermediate state.
    std::shared_ptr<std::mutex> fmutex = file_lock_map::get_instance()->get_mutex(path);
    std::lock_guard<std::mutex> lock(*fmutex);

    // Check and see if the file/directory exists locally (because it's being buffered.)  If so, skip the call to Storage.
    std::string pathString(path);
    std::string mntPathString = prepend_mnt_path_string(pathString);

    int res;
    int acc = access(mntPathString.c_str(), F_OK);
    if (acc != -1)
    {
        AZS_DEBUGLOGV("Accessing mntPath = %s for getattr succeeded; object is in the local cache.\n", mntPathString.c_str());
        //(void) fi;
        res = lstat(mntPathString.c_str(), stbuf);
        if (res == -1)
        {
            int lstaterrno = errno;
            syslog(LOG_ERR, "lstat on file %s in local cache during getattr failed with errno = %d.\n", mntPathString.c_str(), lstaterrno);
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
        AZS_DEBUGLOGV("Object %s is not in the local cache during getattr.\n", mntPathString.c_str());
    }

    // It's not in the local cache.  Check to see if it's a blob on the service:
    std::string blobNameStr(&(path[1]));
    errno = 0;
    BfsFileProperty blob_property = storage_client->GetProperties(blobNameStr);
    mode_t perms = blob_property.m_file_mode == 0 ?  str_options.defaultPermission : blob_property.m_file_mode;

    if ((errno == 0) && blob_property.isValid())
    {
        if (is_directory_blob(blob_property.size(), blob_property.m_metadata))
        {
            AZS_DEBUGLOGV("Blob %s, representing a directory, found during get_attr.\n", path);
            stbuf->st_mode = S_IFDIR | perms;
            // If st_nlink = 2, means directory is empty.
            // Directory size will affect behaviour for mv, rmdir, cp etc.
            stbuf->st_uid = fuse_get_context()->uid;
            stbuf->st_gid = fuse_get_context()->gid;
            stbuf->st_nlink = storage_client->IsDirectory(blobNameStr.c_str()) == D_EMPTY ? 2 : 3;
            stbuf->st_size = 4096;
            return 0;
        }

        AZS_DEBUGLOGV("Blob %s, representing a file, found during get_attr.\n", path);
        stbuf->st_mode = S_IFREG | perms; // Regular file (not a directory)
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
        int dirSize = is_directory_blob(blob_property.size(), blob_property.m_metadata);
        if (errno != 0)
        {
            int storage_errno = errno;
            syslog(LOG_ERR, "Failure when attempting to determine if directory %s exists on the service.  errno = %d.\n", blobNameStr.c_str(), storage_errno);
            return 0 - map_errno(storage_errno);
        }
        if (dirSize != D_NOTEXIST)
        {
            AZS_DEBUGLOGV("Directory %s found on the service.\n", blobNameStr.c_str());
            stbuf->st_mode = S_IFDIR | str_options.defaultPermission;
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
        if(errno == 404)
        {
            // The file does not currently exist on the service or in the cache
            // If the command they are calling is just checking for existence, fuse will call the next operation
            // dependent on this error number. If the command cannot continue without the existence it will print out
            // the correct error to the user.
            syslog(LOG_WARNING, "File does not currently exist on the storage or cache");
            return -(ENOENT);
        }
        // If we received a different error, then let's fail with that error
        int storage_errno = errno;
        syslog(LOG_ERR, "Failure when attempting to determine if %s exists on the service.  errno = %d.\n", blobNameStr.c_str(), storage_errno);
        return 0 - map_errno(storage_errno);
    }

    return ENOSYS;
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

int azs_chmod(const char * path, mode_t mode)
{
    AZS_DEBUGLOGV("azs_chmod called with path = %s, mode = %o.\n", path, mode);

    errno = 0;
    storage_client->ChangeMode(path, mode);

    return errno;
}

//#ifdef HAVE_UTIMENSAT
int azs_utimens(const char * /*path*/, const struct timespec [2] /*ts[2]*/)
{
    //TODO: Implement
//    return -ENOSYS;
    return 0;
}
//  #endif

// TODO: Fix bug where the files and directories in the source in the file cache are not deleted.
// TODO: Fix bugs where the a file has been created but not yet uploaded.
// TODO: Fix the bug where this fails for multi-level dirrectories.
// TODO: If/when we upgrade to FUSE 3.0, we will need to worry about the additional possible flags (RENAME_EXCHANGE and RENAME_NOREPLACE)
int azs_rename(const char *src, const char *dst)
{
    AZS_DEBUGLOGV("azs_rename called with src = %s, dst = %s.\n", src, dst);

    errno = 0;
    storage_client->Rename(src,dst);

    return errno;
}


int azs_setxattr(const char * path, const char * name, const char * value, size_t size, int flags)
{
    AZS_DEBUGLOGV("azs_setxattr (NOT IMPLEMENTED) called with path = %s, name = %s, value = %s., size = %d, flags = %d .\n",
            path,
            name,
            value,
            (int)size,
            flags );
    return -ENOSYS;
}
int azs_getxattr(const char * path, const char * name, char * value, size_t size)
{
    AZS_DEBUGLOGV("azs_getxattr (NOT IMPLEMENTED) called with path = %s, name = %s, value = %s, size = %d .\n",
                  path,
                  name,
                  value,
                  (int)size);
    return 0;
}
int azs_listxattr(const char * path, char * list, size_t size)
{
    AZS_DEBUGLOGV("azs_getxattr (NOT IMPLEMENTED) called with path = %s, list = %s, size = %d .\n",
                  path,
                  list,
                  (int)size);
    return -ENOSYS;
}
int azs_removexattr(const char * path, const char * name)
{
    AZS_DEBUGLOGV("azs_getxattr (NOT IMPLEMENTED) called with path = %s, name = %s .\n",
                  path,
                  name);
    return -ENOSYS;
}

int azs_getdir(const char* path, fuse_dirh_t /*dirh_t*/, fuse_dirfil_t /*dirfil_t*/)
{
    AZS_DEBUGLOGV("azs_getdir (NOT IMPLEMENTED, deprecated) called with path = %s.\n",
                  path);
    return -ENOSYS;
}
int azs_mknod(const char* path, mode_t mode, dev_t /*rdev*/)
{
    AZS_DEBUGLOGV("azs_mknod (NOT IMPLEMENTED) called with path = %s, mode = %d.\n",
                  path,
                  mode);
    return -ENOSYS;
}
int azs_symlink(const char* linkname, const char* name)
{
    AZS_DEBUGLOGV("azs_symlink (NOT IMPLEMENTED) called with linkname = %s, name = %s .\n",
                  linkname,
                  name);
    return -ENOSYS;
}
int azs_link(const char* oldpath, const char* newpath)
{
    AZS_DEBUGLOGV("azs_link (NOT IMPLEMENTED) called with old path = %s,new path = %s .\n",
                  oldpath,
                  newpath);
    return -ENOSYS;
}
int azs_utime(const char* path, utimbuf* tim)
{
    AZS_DEBUGLOGV("azs_utime (NOT IMPLEMENTED) called with off = %s, access time = %d, mod time = %d.\n",
                  path,
                  (int)tim->actime,
                  (int)tim->modtime);
    return -ENOSYS;
}
int azs_fsyncdir(const char* path, int datasync ,struct fuse_file_info * /*fi*/)
{
    AZS_DEBUGLOGV("azs_fsyncdir (NOT IMPLEMENTED) called with off = %s, datasync = %d .\n",
                  path,
                  datasync);
    return -ENOSYS;
}
int azs_ftruncate(const char* path, off_t off, struct fuse_file_info */*fi*/)
{
    AZS_DEBUGLOGV("azs_ftruncate (NOT IMPLEMENTED) called with path = %s, offset = %d .\n",
                  path,
                  (int)off);
    return -ENOSYS;
}
int azs_bmap(const char* path, size_t blocksize, uint64_t* idx)
{
    AZS_DEBUGLOGV("azs_bmap (NOT IMPLEMENTED) called with path = %s, blocksize = %d, index = %d .\n",
                  path,
                  (int)blocksize,
                  (int)*idx);
    return -ENOSYS;
}
int azs_ioctl(const char* path, int cmd, void * /*arg*/, fuse_file_info * /*fi*/, unsigned int flags, void* /*data*/)
{
    AZS_DEBUGLOGV("azs_bmap (NOT IMPLEMENTED) called with path = %s, cmd = %d, flag = %d .\n",
                  path,
                  cmd,
                  flags);
    return -ENOSYS;
}
int azs_poll(const char * path, struct fuse_file_info * /*fi*/, struct fuse_pollhandle * /*ph*/, unsigned int *reventsp)
{
    AZS_DEBUGLOGV("azs_poll (NOT IMPLEMENTED) called with path = %s, reventsp = %d .\n",
                  path,
                  *reventsp);
    return -ENOSYS;
}
int azs_write_buf(const char * path, struct fuse_bufvec * /*buf*/, off_t off, struct fuse_file_info * /*fi*/)
{
    AZS_DEBUGLOGV("azs_write_buf (NOT IMPLEMENTED) called with path = %s, offset = %d .\n",
                  path,
                  (int)off);
    return -ENOSYS;
}
int azs_read_buf(const char * path, struct fuse_bufvec ** /*buf*/, size_t size, off_t off, struct fuse_file_info * /*fi*/)
{
    AZS_DEBUGLOGV("azs_read_buf (NOT IMPLEMENTED) called with path = %s,size = %d, offset = %d .\n",
                  path,
                  (int)size,
                  (int)off);
    return -ENOSYS;
}
int azs_flock(const char * path, struct fuse_file_info * /*fi*/, int op)
{
    AZS_DEBUGLOGV("azs_flock (NOT IMPLEMENTED) called with path = %s,op = %d .\n",
                  path,
                  op);
    return -ENOSYS;
}
int azs_fallocate(const char * path, int mode, off_t off, off_t length, struct fuse_file_info * /*fi*/)
{
    AZS_DEBUGLOGV("azs_write_buf (NOT IMPLEMENTED) called with path = %s,mode = %d, offset = %d, length = %d .\n",
                  path,
                  mode,
                  (int)off,
                  (int)length);
    return -ENOSYS;
}

int azs_lock(const char * path, struct fuse_file_info * /*fi*/, int cmd, struct flock* /*locks*/)
{
    AZS_DEBUGLOGV("azs_lock (NOT IMPLEMENTED) called with path = %s,cmd = %d .\n",
                  path,
                  (int)cmd);
    return -ENOSYS;
}