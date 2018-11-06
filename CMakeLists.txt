cmake_minimum_required(VERSION 3.3)
if(DEFINED ENV{CMAKE_MODULE_PATH})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CMAKE_MODULE_PATH})
endif()

project(NYSE_Ingestor)

# Set C++14 as required standard for all C++ targets.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(NYSE_Ingestor src/main.cc src/Master.cc src/Quote.cc src/Trade.cc src/Array.cc)

target_include_directories(NYSE_Ingestor PUBLIC src)

find_package(TileDB REQUIRED)
find_package(Threads REQUIRED)
find_package(date REQUIRED)
target_link_libraries(NYSE_Ingestor TileDB::tiledb_shared dl ${CMAKE_THREAD_LIBS_INIT} tz)