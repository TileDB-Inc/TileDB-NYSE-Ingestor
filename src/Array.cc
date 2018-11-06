/**
 * @file  Array.cc
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
 * Represents a generic tiledb array for ingestion for NYSE data
 *
 */

#include "Array.h"
#include "buffer.h"
#include <tiledb/tiledb>
#include <CLI11.hpp>
#include <chrono>
#include <ProgressBar.hpp>
#include <utils.h>
#include <ThreadPool.h>
#include <date/tz.h>

#ifdef __LINUX__
#include <sys/ioctl.h>
#endif

std::vector<std::string> nyse::Array::parserHeader(std::string headerLine, char delimiter) {
    return split(headerLine, delimiter);
}

std::shared_ptr<void> nyse::createBuffer(tiledb_datatype_t datatype) {
    switch (datatype) {
        case tiledb_datatype_t::TILEDB_INT32:
            return std::make_shared<std::vector<int32_t>>();
        case tiledb_datatype_t::TILEDB_INT64:
            return std::make_shared<std::vector<int64_t>>();
        case tiledb_datatype_t::TILEDB_FLOAT32:
            return std::make_shared<std::vector<float>>();
        case tiledb_datatype_t::TILEDB_FLOAT64:
            return std::make_shared<std::vector<double>>();
        case tiledb_datatype_t::TILEDB_CHAR:
            return std::make_shared<std::vector<char>>();
        case tiledb_datatype_t::TILEDB_INT8:
            return std::make_shared<std::vector<int8_t>>();
        case tiledb_datatype_t::TILEDB_UINT8    :
            return std::make_shared<std::vector<uint8_t>>();
        case tiledb_datatype_t::TILEDB_INT16:
            return std::make_shared<std::vector<int16_t>>();
        case tiledb_datatype_t::TILEDB_UINT16:
            return std::make_shared<std::vector<uint16_t>>();
        case tiledb_datatype_t::TILEDB_UINT32:
            return std::make_shared<std::vector<uint32_t>>();
        case tiledb_datatype_t::TILEDB_UINT64:
            return std::make_shared<std::vector<uint64_t>>();
        case tiledb_datatype_t::TILEDB_STRING_ASCII:
            return std::make_shared<std::vector<uint8_t>>();
        case tiledb_datatype_t::TILEDB_STRING_UTF8:
            return std::make_shared<std::vector<uint8_t>>();
        case tiledb_datatype_t::TILEDB_STRING_UTF16:
            return std::make_shared<std::vector<uint16_t>>();
        case tiledb_datatype_t::TILEDB_STRING_UTF32:
            return std::make_shared<std::vector<uint32_t>>();
        case tiledb_datatype_t::TILEDB_STRING_UCS2:
            return std::make_shared<std::vector<uint16_t>>();
        case tiledb_datatype_t::TILEDB_STRING_UCS4:
            return std::make_shared<std::vector<uint32_t>>();
        case tiledb_datatype_t::TILEDB_ANY:
            return std::make_shared<std::vector<int8_t>>();
    }
}

static std::unordered_map<std::string, std::shared_ptr<nyse::buffer>> staticLoadJob(nyse::Array *array, std::string file_uri,
                                                                                    std::unordered_map<std::string, std::string> staticColumns,
                                                                                    std::unordered_map<std::string, std::string> ignoreColumns,
                                                                                    std::set<std::string> dimensionFields,
                                                                                    char delimiter,
                                                                                    tiledb::ArraySchema arraySchema) {
    return array->parseFileToBuffer(file_uri, staticColumns, ignoreColumns, dimensionFields, delimiter, arraySchema);
}

std::unordered_map<std::string, std::shared_ptr<nyse::buffer>> nyse::Array::parseFileToBuffer(std::string file_uri,
        std::unordered_map<std::string, std::string> staticColumns,
        std::unordered_map<std::string, std::string> ignoreColumns,
        std::set<std::string> dimensionFields,
        char delimiter,
        tiledb::ArraySchema arraySchema) {
    uint64_t totalRowsInFile = 0;
    std::unordered_map<std::string, std::shared_ptr<buffer>> buffers;
    std::ifstream is(file_uri);
    if (!is.good()) throw std::runtime_error("Error opening " + file_uri);

    // Get number of lines for progressbar
    std::cout << "Getting number of lines in file: " << file_uri << std::endl;
    long linesInFile = std::count(std::istreambuf_iterator<char>(is),
                                  std::istreambuf_iterator<char>(), '\n');
    is.seekg(0, std::ios::beg);

    // Read all contents from the file
    std::string headerLine;
    std::getline(is, headerLine);

    // Parse the header row, this is a virtual function because Trade data needs to remove spaces from header columns
    const std::vector<std::string> &headerFields = this->parserHeader(headerLine, delimiter);
    std::unordered_map<std::string, int> fieldLookup;
    // Validate that there are no extra fields in the file being loaded
    for (int fieldIndex = 0; fieldIndex < headerFields.size(); fieldIndex++) {
        const std::string &field = headerFields[fieldIndex];
        if (dimensionFields.find(field) == dimensionFields.end() && field != "Time") {
            if (arraySchema.attribute(field) == nullptr) {
                std::cerr << field << " does not exists in array (" + array_uri + ")" << std::endl;
                return buffers;
            }
        }
        fieldLookup.emplace(field, fieldIndex);
    }

    buffers = initBuffers(headerFields, staticColumns);

    // Check to see if all dimensions are in file being loaded
    for (const std::string &dimension : dimensionFields) {
        // First check to see if dimension is in static map
        if (staticColumns.find(dimension) != staticColumns.end() || dimension == "symbol_id")
            continue;

        bool found = false;
        for (const std::string &field : headerFields) {
            if (field == dimension) {
                found = true;
                break;
            }
        }
        if (!found) {
            std::cerr << "Dimension " << dimension << " is not present in data file! Aborting loading" << std::endl;
            return buffers;
        }
    }

    uint32_t windowSize = 70;
#ifdef __LINUX__
    struct winsize size;
        ioctl(STDOUT_FILENO,TIOCGWINSZ,&size);
        windowSize = size.ws_row;
        std::cout << "window size: " << windowSize << std::endl;
#endif

    // Create progress bar
    ProgressBar progressBar(file_uri, linesInFile - 1, windowSize);

    int rowsParsed = 0;
    unsigned long expectedFields = dimensionFields.size() /*- staticColumns.size()*/ + arraySchema.attribute_num();
    std::cout << "starting parsing for " << file_uri << " which is " << linesInFile << " rows using batch size " << std::endl;
              //<< batchSize << std::endl;
    for (std::string line; std::getline(is, line);) {
        std::vector<std::string> fields = split(line, delimiter);
        // Trade and quote have a special end file line
        if (totalRowsInFile == linesInFile - 2 && line.substr(0, 3) == "END") {
            break;
        }
        //totalRows++;
        totalRowsInFile++;
        if (fields.size() != expectedFields) {
            std::cerr << "Line " << totalRowsInFile << " is missing fields! Line has " << fields.size() << " expected "
                      << expectedFields << std::endl;
        }
        for (size_t fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
            const std::string &fieldName = headerFields[fieldNum];
            // Skip dimensions
            if (dimensionFields.find(fieldName) != dimensionFields.end())
                continue;
            if (fieldName == "Time")
                continue;
            std::string fieldValue = fields[fieldNum];
            appendBuffer(fieldName, fieldValue, buffers.find(fieldName)->second);
        }

        for (const tiledb::Dimension &dimension : arraySchema.domain().dimensions()) {
            std::string value;
            auto fieldLookupEntry = fieldLookup.find(dimension.name());
            if (fieldLookupEntry != fieldLookup.end()) {
                int fieldIndex = fieldLookupEntry->second;
                value = fields[fieldIndex];
            } else if (dimension.name() == "symbol_id") {
                value = std::to_string(totalRowsInFile);
            } else if (dimension.name() == "datetime") {
                auto timeLookupEntry = fieldLookup.find("Time");
                if (timeLookupEntry != fieldLookup.end()) {
                    std::string time = fields[timeLookupEntry->second];
                    std::string nanoseconds_str = time.substr(time.length()-9, 9);
                    std::string hms = time.substr(0, time.length()-9);
                    std::string date = staticColumnsForFiles.find(file_uri)->second.find("date")->second;
                    std::tm tm = {};
                    std::string datetime = date + hms + "-0400";

                    date::sys_time<std::chrono::nanoseconds> t;
                    std::istringstream stream{datetime};
                    stream >> date::parse("%Y%m%d%H%M%S%z", t);
                    if (stream.fail())
                        throw std::runtime_error("failed to parse " + datetime);


                    auto nanoseconds = std::chrono::nanoseconds(std::stoll(nanoseconds_str));
                    auto finalTime = t + nanoseconds;

                    auto UTC = date::make_zoned("GMT", finalTime).get_local_time();
                    value = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(UTC.time_since_epoch()).count());
                }
            } else {
                auto staticColumnsForFile = staticColumnsForFiles.find(file_uri);
                if (staticColumnsForFile == staticColumnsForFiles.end()) {
                    std::cout << "Warning " << file_uri << " was missing static column mapping for row "
                              << totalRowsInFile << ". Aborting!!" << std::endl;
                    return buffers;
                }
                value = staticColumnsForFiles.find(file_uri)->second.find(dimension.name())->second;
            }
            appendBuffer(dimension.name(), value, buffers.find(TILEDB_COORDS)->second);
        }

        rowsParsed++;
        ++progressBar;
        progressBar.display();
    }
    progressBar.done();
    is.close();
    return buffers;
}

int nyse::Array::load(const std::vector<std::string> file_uris, char delimiter, uint64_t batchSize, uint32_t threads) {
    //tiledb::VFS vfs(ctx);
    //tiledb::VFS::filebuf buff(vfs);
    unsigned long totalRows = 0;

    ThreadPool pool(threads);
    std::vector<std::future<std::unordered_map<std::string, std::shared_ptr<buffer>>>> results;

    array = std::make_unique<tiledb::Array>(*ctx, array_uri, tiledb_query_type_t::TILEDB_WRITE);
    query = std::make_unique<tiledb::Query>(*ctx, *array);

    query->set_layout(tiledb_layout_t::TILEDB_UNORDERED);

    tiledb::ArraySchema arraySchema = array->schema();

    std::set<std::string> dimensionFields;
    for(const tiledb::Dimension &dimension : arraySchema.domain().dimensions()) {
        dimensionFields.emplace(dimension.name());
    }

    //buff.open(file_uri, std::ios::in);

    auto startTime = std::chrono::steady_clock::now();

    //std::istream is(&buff);
    for (const std::string &file_uri : file_uris) {
        if (staticColumnsForFiles.find(file_uri) == staticColumnsForFiles.end())  {
            std::cerr << "File " << file_uri << " missing static columns mapping!! Aborting!!" << std::endl;
            return -1;
        }
        std::unordered_map<std::string, std::string> ignoreColumns;
        if (ignoreColumnsForFiles.find(file_uri) != ignoreColumnsForFiles.end())
            ignoreColumns = ignoreColumnsForFiles.find(file_uri)->second;
        std::unordered_map<std::string, std::string> staticColumns = staticColumnsForFiles.find(file_uri)->second;
        //auto result = pool.enqueue(loadJob(file_uri, staticColumns, dimensionFields));
        results.push_back(pool.enqueue(staticLoadJob, this, file_uri, staticColumns, ignoreColumns, dimensionFields, delimiter, arraySchema));
    }

    for(auto &result : results) {
        auto buffers = result.get();
        for (auto entry : buffers) {
            std::string bufferName = entry.first;
            if (bufferName == TILEDB_COORDS) {
                totalRows += sizeof(entry.second->values) / tiledb_datatype_size(entry.second->datatype) / dimensionFields.size();
            }
            auto globalBuffer = globalBuffers.find(bufferName);
            if (globalBuffer != globalBuffers.end()) {
                if (entry.second->offsets != nullptr) {
                    //globalBuffer->second->offsets->insert( globalBuffer->second->offsets->end(), entry.second->offsets->begin(), entry.second->offsets->end() );
                    /*size_t scaleFactor = std::static_pointer_cast<std::vector<void>>(globalBuffer->second->values)->size();
                    for(auto offset : *entry.second->offsets) {
                        globalBuffer->second->offsets->push_back(offset + scaleFactor);
                    }*/
                    concatOffsets(globalBuffer->second->offsets, entry.second->offsets, globalBuffer->second->values, entry.second->datatype);
                    entry.second->offsets.reset();
                }
                if (entry.second->values != nullptr) {
                    concatBuffers(globalBuffer->second->values, entry.second->values, entry.second->datatype);
                    entry.second->values.reset();
                }
            } else {
                globalBuffers.emplace(bufferName, entry.second);
            }
        }
        //if (globalBuffers.begin()->second->values)
    }

    if (submit_query() == tiledb::Query::Status::FAILED) {
        std::cerr << "Query FAILED!!!!!" << std::endl;
    }

    query->finalize();

    array->close();

    auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime);
    printf("loaded %ld rows in %s (%.2f rows/second)\n",totalRows, beautify_duration(duration).c_str(), (float(totalRows)) / duration.count());

    //buff.close();
    return 0;
}

void nyse::Array::appendBuffer(const std::string &fieldName, const std::string &valueConst, std::shared_ptr<buffer> buffer) {
    std::string value = valueConst;
    // Right now missing values are set to -1 because -1 is not valid in NYSE data set
    if (value.empty())
        value = "-1";
    switch (buffer->datatype) {
        case tiledb_datatype_t::TILEDB_INT32: {
            int32_t valueParsed = std::stoi(value);
            return appendBuffer<int32_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_INT64: {
            int64_t valueParsed = std::stoll(value);
            return appendBuffer<int64_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_FLOAT32: {
            float valueParsed = std::stof(value);
            return appendBuffer<float>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_FLOAT64: {
            double valueParsed = std::stod(value);
            return appendBuffer<double>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_INT8: {
            int8_t valueParsed = static_cast<int8_t>(std::stoi(value));
            return appendBuffer<int8_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_UINT8: {
            uint8_t valueParsed = static_cast<uint8_t>(std::stoi(value));
            return appendBuffer<uint8_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_INT16: {
            int16_t valueParsed = static_cast<int16_t>(std::stoi(value));
            return appendBuffer<int16_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_UINT16: {
            uint16_t valueParsed = static_cast<uint16_t>(std::stoi(value));
            return appendBuffer<uint16_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_UINT32: {
            uint32_t valueParsed = static_cast<uint32_t>(std::stoi(value));
            return appendBuffer<uint32_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_UINT64: {
            uint64_t valueParsed = std::stoull(value);
            return appendBuffer<uint64_t>(fieldName, valueParsed, buffer);
        }
        case tiledb_datatype_t::TILEDB_CHAR:
        case tiledb_datatype_t::TILEDB_STRING_ASCII:
        case tiledb_datatype_t::TILEDB_STRING_UTF8:
        case tiledb_datatype_t::TILEDB_STRING_UTF16:
        case tiledb_datatype_t::TILEDB_STRING_UTF32:
        case tiledb_datatype_t::TILEDB_STRING_UCS2:
        case tiledb_datatype_t::TILEDB_STRING_UCS4:
        case tiledb_datatype_t::TILEDB_ANY: {
            // Handle strings
            std::shared_ptr<std::vector<char>> values = std::static_pointer_cast<std::vector<char>>(buffer->values);
            if (buffer->offsets != nullptr) {
                // We will set empty strings to the word NULL.
                if (valueConst.empty()) {
                    value = "NULL";
                }
                buffer->offsets->push_back(values->size());
            }
            for(const char &c : value) {
                values->emplace_back(c);
            }
        return;
        }
    }
}

template <typename T>
void nyse::Array::appendBuffer(const std::string &fieldName, const T value, std::shared_ptr<buffer> buffer) {
    std::shared_ptr<std::vector<T>> values = std::static_pointer_cast<std::vector<T>>(buffer->values);
    if (buffer->offsets != nullptr) {
        buffer->offsets->push_back(values->size());
    }
    values->push_back(value);
}

tiledb::Query::Status nyse::Array::submit_query() {
    for(auto entry : globalBuffers) {
        std::shared_ptr<buffer> buffer = entry.second;
        switch (buffer->datatype) {
            case tiledb_datatype_t::TILEDB_INT32: {
                std::shared_ptr<std::vector<int32_t >> values = std::static_pointer_cast<std::vector<int32_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_INT64: {
                std::shared_ptr<std::vector<int64_t>> values = std::static_pointer_cast<std::vector<int64_t >>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_FLOAT32: {
                std::shared_ptr<std::vector<float>> values = std::static_pointer_cast<std::vector<float>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_FLOAT64: {
                std::shared_ptr<std::vector<double>> values = std::static_pointer_cast<std::vector<double>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_INT8: {
                std::shared_ptr<std::vector<int8_t>> values = std::static_pointer_cast<std::vector<int8_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_UINT8: {
                std::shared_ptr<std::vector<uint8_t>> values = std::static_pointer_cast<std::vector<uint8_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_INT16: {
                std::shared_ptr<std::vector<int16_t>> values = std::static_pointer_cast<std::vector<int16_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_UINT16: {
                std::shared_ptr<std::vector<uint16_t>> values = std::static_pointer_cast<std::vector<uint16_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_UINT32: {
                std::shared_ptr<std::vector<uint32_t>> values = std::static_pointer_cast<std::vector<uint32_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_UINT64: {
                std::shared_ptr<std::vector<uint64_t>> values = std::static_pointer_cast<std::vector<uint64_t>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
            case tiledb_datatype_t::TILEDB_CHAR:
            case tiledb_datatype_t::TILEDB_STRING_ASCII:
            case tiledb_datatype_t::TILEDB_STRING_UTF8:
            case tiledb_datatype_t::TILEDB_STRING_UTF16:
            case tiledb_datatype_t::TILEDB_STRING_UTF32:
            case tiledb_datatype_t::TILEDB_STRING_UCS2:
            case tiledb_datatype_t::TILEDB_STRING_UCS4:
            case tiledb_datatype_t::TILEDB_ANY: {
                std::shared_ptr<std::vector<char>> values = std::static_pointer_cast<std::vector<char>>(buffer->values);
                if (buffer->offsets != nullptr) {
                    query->set_buffer(entry.first, *buffer->offsets, *values);
                } else {
                    query->set_buffer(entry.first, *values);
                }
                break;
            }
        }

    }
    return query->submit();
}


std::unordered_map<std::string, std::shared_ptr<nyse::buffer>> nyse::Array::initBuffers(std::vector<std::string> headerFields, std::unordered_map<std::string, std::string> staticColumns) {
    std::unordered_map<std::string, std::shared_ptr<buffer>> buffers;

    tiledb::ArraySchema arraySchema = array->schema();
    tiledb_datatype_t domainType = arraySchema.domain().type();
    buffers.emplace(TILEDB_COORDS, std::make_shared<buffer>(buffer{nullptr, createBuffer(domainType), domainType}));

    // Check to see if all attributes are in file being loaded
    for (std::pair<const std::string, tiledb::Attribute> &entry : arraySchema.attributes()) {
        // First check to see if attribute is in static map
        if(staticColumns.find(entry.first) != staticColumns.end())
            continue;

        bool found = false;
        for (const std::string &field : headerFields) {
            if (field == entry.first) {
                found = true;
                break;
            }
        }
        if (!found) {
            std::cerr << "Attribute " << entry.first << " is not present in data file! Aborting loading" << std::endl;
            return buffers;
        }
        tiledb_datatype_t type = entry.second.type();
        std::shared_ptr<void> valueBuffer = createBuffer(type);
        if (entry.second.variable_sized()) {
            buffers.emplace(entry.first, std::make_shared<buffer>(buffer{std::static_pointer_cast<std::vector<uint64_t>>(createBuffer(TILEDB_UINT64)), valueBuffer, type}));
        } else {
            buffers.emplace(entry.first, std::make_shared<buffer>(buffer{nullptr, valueBuffer, type}));
        }
    }

    return buffers;
}

void nyse::Array::concatBuffers(std::shared_ptr<void> globalBuffer, std::shared_ptr<void> bufferToAppend,
                                tiledb_datatype_t datatype) {
    switch (datatype) {
        case tiledb_datatype_t::TILEDB_INT32: {
            std::shared_ptr<std::vector<int32_t >> globalBufferCast = std::static_pointer_cast<std::vector<int32_t>>(globalBuffer);
            std::shared_ptr<std::vector<int32_t >> bufferToAppendCast = std::static_pointer_cast<std::vector<int32_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());

            break;
        }
        case tiledb_datatype_t::TILEDB_INT64: {
            std::shared_ptr<std::vector<int64_t>> globalBufferCast = std::static_pointer_cast<std::vector<int64_t>>(globalBuffer);
            std::shared_ptr<std::vector<int64_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<int64_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_FLOAT32: {
            std::shared_ptr<std::vector<float>> globalBufferCast = std::static_pointer_cast<std::vector<float>>(globalBuffer);
            std::shared_ptr<std::vector<float>> bufferToAppendCast = std::static_pointer_cast<std::vector<float>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_FLOAT64: {
            std::shared_ptr<std::vector<double>> globalBufferCast = std::static_pointer_cast<std::vector<double>>(globalBuffer);
            std::shared_ptr<std::vector<double>> bufferToAppendCast = std::static_pointer_cast<std::vector<double>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_INT8: {
            std::shared_ptr<std::vector<int8_t>> globalBufferCast = std::static_pointer_cast<std::vector<int8_t>>(globalBuffer);
            std::shared_ptr<std::vector<int8_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<int8_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT8: {
            std::shared_ptr<std::vector<uint8_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint8_t>>(globalBuffer);
            std::shared_ptr<std::vector<uint8_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<uint8_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_INT16: {
            std::shared_ptr<std::vector<int16_t>> globalBufferCast = std::static_pointer_cast<std::vector<int16_t>>(globalBuffer);
            std::shared_ptr<std::vector<int16_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<int16_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT16: {
            std::shared_ptr<std::vector<uint16_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint16_t>>(globalBuffer);
            std::shared_ptr<std::vector<uint16_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<uint16_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT32: {
            std::shared_ptr<std::vector<uint32_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint32_t>>(globalBuffer);
            std::shared_ptr<std::vector<uint32_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<uint32_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT64: {
            std::shared_ptr<std::vector<uint64_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint64_t>>(globalBuffer);
            std::shared_ptr<std::vector<uint64_t>> bufferToAppendCast = std::static_pointer_cast<std::vector<uint64_t>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
        case tiledb_datatype_t::TILEDB_CHAR:
        case tiledb_datatype_t::TILEDB_STRING_ASCII:
        case tiledb_datatype_t::TILEDB_STRING_UTF8:
        case tiledb_datatype_t::TILEDB_STRING_UTF16:
        case tiledb_datatype_t::TILEDB_STRING_UTF32:
        case tiledb_datatype_t::TILEDB_STRING_UCS2:
        case tiledb_datatype_t::TILEDB_STRING_UCS4:
        case tiledb_datatype_t::TILEDB_ANY: {
            std::shared_ptr<std::vector<char>> globalBufferCast = std::static_pointer_cast<std::vector<char>>(globalBuffer);
            std::shared_ptr<std::vector<char>> bufferToAppendCast = std::static_pointer_cast<std::vector<char>>(bufferToAppend);
            globalBufferCast->insert(globalBufferCast->end(), bufferToAppendCast->begin(), bufferToAppendCast->end());
            break;
        }
    }
}

void nyse::Array::concatOffsets(std::shared_ptr<std::vector<uint64_t>> globalOffsets,
        std::shared_ptr<std::vector<uint64_t>> bufferOffsets,
        std::shared_ptr<void> globalValuesBuffer, tiledb_datatype_t datatype) {
    switch (datatype) {
        case tiledb_datatype_t::TILEDB_INT32: {
            std::shared_ptr<std::vector<int32_t>> globalBufferCast = std::static_pointer_cast<std::vector<int32_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_INT64: {
            std::shared_ptr<std::vector<int64_t>> globalBufferCast = std::static_pointer_cast<std::vector<int64_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_FLOAT32: {
            std::shared_ptr<std::vector<float>> globalBufferCast = std::static_pointer_cast<std::vector<float>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_FLOAT64: {
            std::shared_ptr<std::vector<double>> globalBufferCast = std::static_pointer_cast<std::vector<double>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_INT8: {
            std::shared_ptr<std::vector<int8_t>> globalBufferCast = std::static_pointer_cast<std::vector<int8_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT8: {
            std::shared_ptr<std::vector<uint8_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint8_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_INT16: {
            std::shared_ptr<std::vector<int16_t>> globalBufferCast = std::static_pointer_cast<std::vector<int16_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT16: {
            std::shared_ptr<std::vector<uint16_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint16_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT32: {
            std::shared_ptr<std::vector<uint32_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint32_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_UINT64: {
            std::shared_ptr<std::vector<uint64_t>> globalBufferCast = std::static_pointer_cast<std::vector<uint64_t>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
        case tiledb_datatype_t::TILEDB_CHAR:
        case tiledb_datatype_t::TILEDB_STRING_ASCII:
        case tiledb_datatype_t::TILEDB_STRING_UTF8:
        case tiledb_datatype_t::TILEDB_STRING_UTF16:
        case tiledb_datatype_t::TILEDB_STRING_UTF32:
        case tiledb_datatype_t::TILEDB_STRING_UCS2:
        case tiledb_datatype_t::TILEDB_STRING_UCS4:
        case tiledb_datatype_t::TILEDB_ANY: {
            std::shared_ptr<std::vector<char>> globalBufferCast = std::static_pointer_cast<std::vector<char>>(globalValuesBuffer);
            size_t scaleFactor = globalBufferCast->size();
            for(auto offset : *bufferOffsets) {
                globalOffsets->push_back(offset + scaleFactor);
            }
            break;
        }
    }
}

const std::shared_ptr<tiledb::Context> &nyse::Array::getCtx() const {
    return ctx;
}
