#include "StorageBfsClientBase.h"
#include "blob/blob_client.h"


class BlockBlobBfsClient : StorageBfsClientBase
{
public:
    BlockBlobBfsClient(std::string container, str_options str_options) :
    StorageBfsClientBase(container,str_options)
    {}

    ~BlockBlobBfsClient();
    ///<summary>
    /// Uploads contents of a file to a block blob to the Storage service
    ///</summary>
    ///TODO: params
    ///<returns>none</returns>
    void UploadFromFile(std::string sourcePath) override;
    ///<summary>
    /// Uploads contents of a stream to a block blob to the Storage service
    ///</summary>
    ///<returns>none</returns>
    void UploadFromStream(std::istream & sourceStream, std::string blobName) override;
    ///<summary>
    /// Downloads contents of a block blob to a local file
    ///</summary>
    ///<returns>none</returns>
    void DownloadToFile(std::string blobName, std::string filePath) override;
    ///<summary>
    /// Creates a Directory
    ///</summary>
    ///<returns>none</returns>
    bool CreateDirectory(const char * directoryPath) override;
    ///<summary>
    /// Deletes a Directory
    ///</summary>
    ///<returns>none</returns>
    bool DeleteDirectory(const char * directoryPath) override;
    ///<summary>
    /// Checks if the blob is a directory
    ///</summary>
    ///<returns>none</returns>
    bool IsDirectory(const char * path) override;
    ///<summary>
    /// Helper function - Checks if the "directory" blob is empty
    ///</summary>
    Constants::D_RETURN_CODE IsDirectoryEmpty(const char * path) override;
    ///<summary>
    /// Deletes a File
    ///</summary>
    ///<returns>none</returns>
    void DeleteFile(std::string pathToDelete) override;
    ///<summary>
    /// Gets the properties of a path
    ///</summary>
    ///<returns>BfsFileProperty object which contains the property details of the file</returns>
    BfsFileProperty GetProperties(std::string pathName) override;
    ///<summary>
    /// Determines whether or not a path (file or directory) exists or not
    ///</summary>
    ///<returns>none</returns>
    int Exists(std::string pathName) override;
    ///<summary>
    /// Determines whether or not a path (file or directory) exists or not
    ///</summary>
    ///<returns>none</returns>
    bool Copy(std::string sourcePath, std::string destinationPath) override;
    ///<summary>
    /// Renames a file
    ///</summary>
    ///<returns>none</returns>
    std::vector<std::string> Rename(std::string sourcePath, std::string destinationPath) override;
    ///<summary>
    /// Lists
    ///</summary>
    ///<returns>none</returns>
    list_hierarchical_response List(std::string delimiter, std::string continuation, std::string prefix) const override;
    ///<summary>
    /// LIsts all directories within a list container
    /// Greedily list all blobs using the input params.
    ///</summary>
    std::vector<std::pair<std::vector<list_hierarchical_item>, bool>> ListAllItemsHierarchical(const std::string& delimiter, const std::string& prefix);

private:
    ///<summary>
    /// Helper function - Renames single file
    ///</summary>
    int rename_single_file(const char * src, const char * dst, std::vector<std::string> & files_to_remove_cache);
    ///<summary>
    /// Helper function - Renames directory
    ///</summary>
    int rename_directory(const char * src, const char * dst, std::vector<std::string> & files_to_remove_cache);
    ///<summary>
    /// Helper function - Ensures directory path exists in the cache
    /// TODO: refactoring, rename variables and add comments to make sense to parsing
    ///</summary>
    int ensure_directory_path_exists_cache(const std::string & file_path);

    std::shared_ptr<sync_blob_client> m_blob_client;
};