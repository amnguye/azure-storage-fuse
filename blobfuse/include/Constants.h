//
// Created by amanda on 1/23/20.
//
#include <string>
#include <map>
#include <syslog.h>

#define AZS_DEBUGLOGV(fmt,...) do {syslog(LOG_DEBUG,"Function %s, in file %s, line %d: " fmt, __func__, __FILE__, __LINE__, __VA_ARGS__); } while(0)
#define AZS_DEBUGLOG(fmt) do {syslog(LOG_DEBUG,"Function %s, in file %s, line %d: " fmt, __func__, __FILE__, __LINE__); } while(0)

#ifndef CONSTANT_H_
#define CONSTANT_H_

enum D_RETURN_CODE
{
    D_NOTEXIST = -1,
    D_EMPTY = 0,
    D_NOTEMPTY = 1,
    D_FAILED = 2
};

enum AUTH_TYPE {
    MSI_AUTH,
    SAS_AUTH,
    KEY_AUTH,
    INVALID_AUTH
};
const std::string former_directory_signifier = ".directory";

// Currently, the cpp lite lib puts the HTTP status code in errno.
// This mapping tries to convert the HTTP status code to a standard Linux errno.
// TODO: Ensure that we map any potential HTTP status codes we might receive.
const std::map<int, int> error_mapping = {{404, ENOENT}, {403, EACCES}, {1600, ENOENT}};

/* Define high and low gc_cache threshold values*/
/* These threshold values were not calculated and are just an approximation of when we should be clearing the cache */
const int HIGH_THRESHOLD_VALUE = 90;
const int LOW_THRESHOLD_VALUE = 80;

/* Defines values for concurrency for making blob wrappers */
const int max_concurrency_oauth = 2;
const int max_retry_oauth = 5;

const int max_concurrency_blob_wrapper = 20;

#endif //CONSTANT_h