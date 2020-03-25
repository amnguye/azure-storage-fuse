#include "StorageBfsClientBase.h"
#include "adls_client.h"
#include "BlockBlobBfsClient.h"

#ifndef DATALAKEBFSCLIENT_H
#define DATALAKEBFSCLIENT_H

class DataLakeBfsClient : public BlockBlobBfsClient
{
public:
    DataLakeBfsClient(str_options str_options) :
    BlockBlobBfsClient(str_options),
    m_adls_client(NULL)
    {}
    ///<summary>
    /// Authenticates the storage account and container
    ///</summary>
    ///<returns>bool: if we authenticate to the storage account and container successfully</returns>
    bool AuthenticateStorage() override;
    ///<summary>
    /// Creates a Directory
    ///</summary>
    ///<returns>none</returns>
    bool CreateDirectory(const std::string directoryPath) override;
    ///<summary>
    /// Deletes a Directory
    ///</summary>
    ///<returns>none</returns>
    bool DeleteDirectory(const std::string directoryPath) override;
    ///<summary>
    /// Helper function - Checks if the "directory" blob is empty
    ///</summary>
    D_RETURN_CODE IsDirectoryEmpty(std::string path) override;
    ///<summary>
    /// Renames a DataLake file
    ///</summary>
    ///<returns>none</returns>
    std::vector<std::string> Rename(std::string sourcePath, std::string destinationPath) override;
    ///<summary>
    /// Lists
    ///</summary>
    ///<returns>none</returns>
    list_hierarchical_response List(std::string continuation, std::string prefix, std::string delimiter = "") override;
    ///<summary>
    /// Updates the UNIX-style file mode on a path.
    ///</summary>
    int ChangeMode(const char* path, mode_t mode) override;
    ///<summary>
    /// Gets the properties of a path
    ///</summary>
    ///<returns>BfsFileProperty object which contains the property details of the file</returns>
    BfsFileProperty GetProperties(std::string pathName) override;
private:
    ///<summary>
    /// Helper function - Authenticates with an account key
    ///</summary>
    std::shared_ptr<microsoft_azure::storage::adls_client> authenticate_adls_accountkey();
    ///<summary>
    /// Helper function - Authenticates with an account sas
    ///</summary>
    std::shared_ptr<microsoft_azure::storage::adls_client> authenticate_adls_sas();
    ///<summary>
    /// Helper function - Authenticates with msi
    ///</summary>
    std::shared_ptr<microsoft_azure::storage::adls_client> authenticate_adls_msi();
    ///<summary>
    /// Helper function - Authenticates with spn
    ///</summary>
    std::shared_ptr<adls_client> authenticate_adls_spn();
    ///<summary>
    /// Helper function - Renames cached files
    ///</summary>
    ///<returns>Error value</return>
    int rename_cached_file(std::string src, std::string dest);
    ///<summary>
    /// ADLS Client to make dfs storage calls
    ///</summary>
    std::shared_ptr<microsoft_azure::storage::adls_client> m_adls_client;
};

#endif //DATALAKEBFSCLIENT_H