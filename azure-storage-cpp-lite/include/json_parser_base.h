#pragma once

#include <string>

#include "common.h"

namespace microsoft_azure {
    namespace storage {

    class json_parser_base
    {
	    public:
	        virtual ~json_parser_base() = 0;

	        template<typename RESPONSE_TYPE>
	        RESPONSE_TYPE parse_response(const std::string &) const { return RESPONSE_TYPE(); }
	    };

    	

}}   // azure::storage_lite
