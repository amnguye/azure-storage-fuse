//
// Created by adreed on 2/6/2020.
//

#include <permissions.h>

std::string modeToString(mode_t mode) {
    //The format for the value x-ms-acl is user::rwx,group::rwx,mask::rwx,other::rwx
    //Since fuse doesn't have a way to expose mask to the user, we only are concerned about
    // user, group and other.
    std::string result = "user::";

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