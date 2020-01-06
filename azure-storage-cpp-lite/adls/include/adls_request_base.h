#pragma once

#include "storage_request_base.h"
#include "storage_account.h"
#include "constants.h"

namespace microsoft_azure { namespace storage_adls {

    using storage_account = microsoft_azure::storage::storage_account;
    using http_base = microsoft_azure::storage::http_base;

    class adls_request_base : public microsoft_azure::storage::storage_request_base
    {
    };

}}  // azure::storage_adls