cmake_minimum_required(VERSION 3.3)
if(DEFINED ENV{CMAKE_MODULE_PATH})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CMAKE_MODULE_PATH})
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

# Set C++14 as required standard for all C++ targets.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Superbuild option must be on by default.
option(SUPERBUILD "If true, perform a superbuild (builds all missing dependencies)." ON)
option(CMAKE_IDE "(Used for CLion builds). Disables superbuild and sets the EP install dir." OFF)

# Release builds by default.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Add some global compiler flags
if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_options(-DDEBUG -O0 -g3 -ggdb3 -gdwarf-3)
elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    add_compile_options(-DNDEBUG -O3)
endif()

#add_compile_options("-DUSE_OS_TZDB=1")
#add_compile_options("-DHAS_STRING_VIEW=0")

############################################################
# Superbuild setup
############################################################

# Search the externals install directory for dependencies.
list(APPEND CMAKE_PREFIX_PATH "${EP_INSTALL_PREFIX}")

# If this is an in-IDE build, we need to disable the superbuild and explicitly
# set the EP base dir. The normal 'cmake && make' process won't need this step,
# it is for better CLion support of the superbuild architecture.
if (CMAKE_IDE)
    set(SUPERBUILD OFF)
    set(EP_BASE "${CMAKE_CURRENT_BINARY_DIR}/externals")
endif()

if (SUPERBUILD)
    project(NYSE_Ingestor-Superbuild)
    message(STATUS "Starting TileDB-Genomics superbuild.")
    include("cmake/Superbuild.cmake")
    return()
endif()


project(NYSE_Ingestor)
message(STATUS "Starting NYSE_Ingestor regular build.")

# Paths to locate the installed external projects.
set(EP_SOURCE_DIR "${EP_BASE}/src")
set(EP_INSTALL_PREFIX "${EP_BASE}/install")

############################################################
# Regular build
############################################################

add_executable(NYSE_Ingestor src/main.cc src/Master.cc src/Quote.cc src/Trade.cc src/Array.cc)

target_include_directories(NYSE_Ingestor PUBLIC src)

find_package(TileDB_EP REQUIRED)
find_package(Threads REQUIRED)
find_package(Date_EP REQUIRED)
target_link_libraries(NYSE_Ingestor dl ${CMAKE_THREAD_LIBS_INIT} Date::tz)

if (TARGET TileDB::tiledb_static)
    target_link_libraries(NYSE_Ingestor TileDB::tiledb_static)
else()
    target_link_libraries(NYSE_Ingestor TileDB::tiledb_shared)
endif()