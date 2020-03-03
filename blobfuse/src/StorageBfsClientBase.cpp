#include "include/StorageBfsClientBase.h"
#include <vector>
//
// Created by amanda on 1/17/20.
//

int StorageBfsClientBase::map_errno(int error)
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

std::string StorageBfsClientBase::prepend_mnt_path_string(const std::string& path)
{
    std::string result;
    result.reserve(configurations.tmpPath.length() + 5 + path.length());
    return result.append(configurations.tmpPath).append("/root").append(path);
}

list_hierarchical_item::list_hierarchical_item(list_blobs_hierarchical_item item) :
        name(item.name),
        snapshot(item.snapshot),
        last_modified(item.last_modified),
        etag(item.etag),
        content_length(item.content_length),
        content_encoding(item.content_encoding),
        content_md5(item.content_md5),
        content_language(item.content_language),
        cache_control(item.cache_control),
        copy_status(item.copy_status),
        metadata(std::move(item.metadata)),
        is_directory(item.is_directory) {}

list_hierarchical_response::list_hierarchical_response(list_blobs_hierarchical_response response) :
        m_ms_request_id(std::move(response.ms_request_id)),
        m_next_marker(std::move(response.next_marker)),
        m_valid(true)
{
    //TODO make this better
    unsigned int item_size = response.blobs.size();
    for(unsigned int i = 0; i < item_size; i++)
    {
        m_items.push_back(list_hierarchical_item(response.blobs.at(i)));
    }
}
