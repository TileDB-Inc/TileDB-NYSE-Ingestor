/**
 * @file   main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * A program for loading delimited files into a tiledb array.
 *
 */

#include <iostream>
#include <sstream>
#include <tiledb/tiledb>
#include <CLI11.hpp>
#include <thread>
#include "Master.h"
#include "Quote.h"
#include "Trade.h"
#include "utils.h"

enum class FileType : int { UNKNOWN, Master, Quote, Trade };

std::istream &operator>>(std::istream &in, FileType &fileType) {
    std::string s;
    in >> s;
    if (s == "master" || s == "Master" || s == "MASTER") {
        fileType = FileType::Master;
    } else if (s == "quote" || s == "Quote" || s == "QUOTE") {
        fileType = FileType::Quote;
    } else if (s == "trade" || s == "Trade" || s == "TRADE") {
        fileType = FileType::Trade;
    } else {
        fileType = FileType::UNKNOWN;
    }
    return in;
}

std::ostream &operator<<(std::ostream &in, const FileType &fileType) { return in << static_cast<int>(fileType); }

int main(int argc, char** argv) {
    CLI::App app{"App description"};

    std::vector<std::string> filename;
    app.add_option("-f,--files", filename, "csv files to load", false);

    std::string arrayUri;
    app.add_option("-a,--array,--array_uri", arrayUri, "URI for array loading", false);

    std::string delimiter = "|";
    app.add_option("-d,--delimiter", delimiter, "delimiter used in file", false);

    FileType fileType;
    app.add_set("--type", fileType, {FileType::Master, FileType::Trade, FileType::Quote}, "File type to ingest")
            ->type_name("FileType in {Master, Quote, Trade}")->required(true);

    bool createArray = false;
    app.add_flag("-c,--create", createArray, "create array and exit");

    uint64_t batchSize = 10000;
    app.add_option("-b,--batch", batchSize, "batch size for bulk loading");

    uint32_t threads = std::thread::hardware_concurrency();
    app.add_option("--threads", threads, "Number of threads for loading in parallel");

    bool consolidate = false;
    app.add_flag("--consolidate", consolidate, "Consolidate array");

    bool readSample = false;
    app.add_flag("--read", readSample, "read sample data from array for testing");

    std::string writeFile;
    app.add_option("--write-file", writeFile, "File to write csv format data from read");

    std::vector<std::string> coordinate_filters;
    app.add_option("--coordinate_filters", coordinate_filters, "List of filters to apply to coordinates", false);

    std::vector<std::string> offset_filters;
    app.add_option("--offset_filters", offset_filters, "List of filters to apply to offsets", false);

    std::vector<std::string> attribute_filters;
    app.add_option("--attribute_filters", attribute_filters, "List of filters to apply to attributes", false);

    CLI11_PARSE(app, argc, argv);

    if (filename.empty() && !createArray && !readSample) {
        std::cerr << "Filename is required unless --create or --read is passed" << std::endl;
        return 0;
    }

    if (fileType == FileType::UNKNOWN) {
        std::cerr << "Unknown filetype passed, must be one of {Master, Quote, Trade}" << std::endl;
        return 0;
    }

    std::unique_ptr<nyse::Array> array;
    if (fileType == FileType::Master) {
        array = std::make_unique<nyse::Master>(arrayUri);
    } else if (fileType == FileType::Quote) {
        array = std::make_unique<nyse::Quote>(arrayUri);
    } else if (fileType == FileType::Trade) {
        array = std::make_unique<nyse::Trade>(arrayUri);
    }
    if (createArray) {

        tiledb::FilterList coordinate_filter_list(*array->getCtx());
        tiledb::FilterList offset_filter_list(*array->getCtx());
        tiledb::FilterList attribute_filter_list(*array->getCtx());
        if (!coordinate_filters.empty()) {
            nyse::create_filter_list_from_str(*array->getCtx(), coordinate_filter_list, coordinate_filters);
        } else {
            coordinate_filter_list.add_filter({*array->getCtx(), TILEDB_FILTER_DOUBLE_DELTA}).add_filter({*array->getCtx(), TILEDB_FILTER_ZSTD});
        }
        if (!offset_filters.empty()) {
            nyse::create_filter_list_from_str(*array->getCtx(), offset_filter_list, offset_filters);
        } else {
            offset_filter_list.add_filter({*array->getCtx(), TILEDB_FILTER_DOUBLE_DELTA}).add_filter({*array->getCtx(), TILEDB_FILTER_ZSTD});
        }
        if (!attribute_filters.empty()) {
            nyse::create_filter_list_from_str(*array->getCtx(), attribute_filter_list, attribute_filters);
        } else {
            attribute_filter_list.add_filter({*array->getCtx(), TILEDB_FILTER_ZSTD});
        }
        array->createArray(coordinate_filter_list, offset_filter_list, attribute_filter_list);
        return 0;
    }


    if (consolidate) {
        auto startTime = std::chrono::steady_clock::now();
        tiledb::Context ctx;
        tiledb::Array::consolidate(ctx, arrayUri);

        auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime);
        printf("consolidated in %s\n", nyse::beautify_duration(duration).c_str());
        return 0;
    }

    if (readSample) {
        auto startTime = std::chrono::steady_clock::now();
        uint64_t rows = array->readSample(writeFile, delimiter);

        auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime);
        printf("read %lu rows in %s (%.2f rows/second)\n", rows, nyse::beautify_duration(duration).c_str(), float(rows) / duration.count());

        return 0;
    }

    return array->load(filename, delimiter.c_str()[0], batchSize, threads);
}