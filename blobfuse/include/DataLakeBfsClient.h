#include "StorageBfsClientBase.h"
#include "adls_client.h"

class DataLakeBfsClient : StorageBfsClientBase
{
public:
    DataLakeBfsClient(std::string container, str_options str_options) :
            StorageBfsClientBase(container,str_options)
    {}

    ~DataLakeBfsClient();
    ///<summary>
    /// Uploads contents of a file to a storage object(e.g. blob, file) to the Storage service
    ///</summary>
    ///TODO: params
    ///<returns>none</returns>
    void UploadFromFile(std::string localPath) override;
    ///<summary>
    /// Uploads contents of a stream to a storage object(e.g. blob, file) to the Storage service
    ///</summary>
    ///<returns>none</returns>
    void UploadFromStream(std::istream & sourceStream, std::string blobName) override;
    ///<summary>
    /// Downloads contents of a storage object(e.g. blob, file) to a local file
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
    /// Determines whether or not a path (file or directory) exists or not
    ///</summary>
    ///<returns>none</returns>
    int Exists(std::string pathName) override;
    ///<summary>
    /// Gets the properties of a path
    ///</summary>
    ///<returns>BfsFileProperty object which contains the property details of the file</returns>
    BfsFileProperty GetProperties(std::string pathName) override;
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
     std::vector<std::pair<std::vector<list_hierarchical_item>, bool>> ListAllItemsHierarchical(const std::string& delimiter, const std::string& prefix) const override;

private:
    std::shared_ptr<microsoft_azure::storage_adls::adls_client> m_adls_client;
};
