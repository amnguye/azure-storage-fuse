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
private:
    ///<summary>
    /// Helper function - Authenticates with an account key
    ///</summary>
    std::shared_ptr<microsoft_azure::storage_adls::adls_client> authenticate_accountkey();
    ///<summary>
    /// Helper function - Authenticates with an account sas
    ///</summary>
    std::shared_ptr<microsoft_azure::storage_adls::adls_client> authenticate_sas();
    ///<summary>
    /// Helper function - Authenticates with msi
    ///</summary>
    std::shared_ptr<microsoft_azure::storage_adls::adls_client> authenticate_msi();
    ///<summary>
    /// ADLS Client to make dfs storage calls
    ///</summary>
    std::shared_ptr<microsoft_azure::storage_adls::adls_client> m_adls_client;
};

#endif //DATALAKEBFSCLIENT_H