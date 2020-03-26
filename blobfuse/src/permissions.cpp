//
// Created by adreed on 2/6/2020.
//

#include <permissions.h>

std::string modeToString(mode_t mode) {
    std::string result = "user::"; // = "user::rwx,group::r--,mask::rwx,other::---";

    result.push_back(mode & (1 << 8) ? 'r': '-');
    result.push_back(mode & (1 << 7) ? 'w' : '-');
    result.push_back(mode & (1 << 6) ? 'x' : '-');

    result += ",group::";
    result.push_back(mode & (1 << 5) ? 'r' : '-');
    result.push_back(mode & (1 << 4) ? 'w' : '-');
    result.push_back(mode & (1 << 3) ? 'x' : '-');

    // Push back the string with each of the mode segments
    result += ",other::";
    result.push_back(mode & (1 << 2) ? 'r' : '-');
    result.push_back(mode & (1 << 1) ? 'w' : '-');
    result.push_back(mode & 01 ? 'w' : '-');

    return result;
}