#
# FindDate_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Finds the Date library, installing with an ExternalProject as necessary.
#
# This module defines:
#   - DATE_INCLUDE_DIR, directory containing headers
#   - DATE_LIBRARIES, the Date library path
#   - DATE_FOUND, whether Date has been found
#   - The Date::Date imported target

# Search the path set during the superbuild for the EP.
set(DATE_PATHS ${EP_INSTALL_PREFIX})

find_path(DATE_INCLUDE_DIR
        NAMES date/tz.h
        PATHS ${DATE_PATHS}
        PATH_SUFFIXES include
        )

find_library(DATE_LIBRARIES
        NAMES tz
        PATHS ${DATE_PATHS}
        PATH_SUFFIXES lib
        )

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Date
        REQUIRED_VARS DATE_LIBRARIES DATE_INCLUDE_DIR
        )

if (NOT DATE_FOUND)
    if (SUPERBUILD)
        message(STATUS "Adding Date as an external project")
        ExternalProject_Add(ep_date
                PREFIX "externals"
                #URL https://github.com/HowardHinnant/date/archive/v2.4.1.tar.gz
                #URL_HASH SHA1=4ed983e1d19ee28bc565bd62907d203304b38cf7
                GIT_REPOSITORY https://github.com/HowardHinnant/date.git
                UPDATE_COMMAND ""
                CMAKE_ARGS
                -DCMAKE_INSTALL_PREFIX=${EP_INSTALL_PREFIX}
                -DCMAKE_PREFIX_PATH=${EP_INSTALL_PREFIX}
                -DUSE_SYSTEM_TZ_DB=ON
                -DDISABLE_STRING_VIEW=ON
                LOG_DOWNLOAD TRUE
                LOG_CONFIGURE TRUE
                LOG_BUILD TRUE
                LOG_INSTALL TRUE
                )

        list(APPEND EXTERNAL_PROJECTS ep_date)
    else()
        message(FATAL_ERROR "Unable to find Date")
    endif()
endif()

# Create the imported target for Date
if (DATE_FOUND AND NOT TARGET Date::tz)
    add_library(Date::tz UNKNOWN IMPORTED)
    set_target_properties(Date::tz PROPERTIES
            IMPORTED_LOCATION "${DATE_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${DATE_INCLUDE_DIR}"
            )
endif()