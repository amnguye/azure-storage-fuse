#pragma once

#include <string>
#include <vector>

#include "json_parser_base.h"
#include "list_paths_request.h"

namespace microsoft_azure { namespace storage {

    class nlohmann_json_parser final : public microsoft_azure::storage::json_parser_base
    {
    public:
        static std::vector<list_paths_item> parse_list_paths_response(const std::string& json);
    };

}}  // azure::storage

namespace microsoft_azure { namespace storage {

    template<>
    inline std::vector<microsoft_azure::storage::list_paths_item> json_parser_base::parse_response(const std::string& json) const
    {
        return microsoft_azure::storage::nlohmann_json_parser::parse_list_paths_response(json);
    }

}}  // azure::storage_lite