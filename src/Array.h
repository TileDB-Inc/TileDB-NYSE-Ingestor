/**
 * @file  Array.h
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

#ifndef NYSE_INGESTOR_ARRAY_H
#define NYSE_INGESTOR_ARRAY_H

#include <string>
#include <tiledb/tiledb>
#include <chrono>
#include <iomanip>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include "buffer.h"

enum class FileType : int { UNKNOWN, Master, Quote, Trade };

namespace nyse {
    /**
     * Helper function for splitting a string on a delimiter
     * @param s string
     * @param delim delimiter
     * @return vector containing split strings
     */
    static std::vector<std::string> split(const std::string &s, char delim) {
        std::stringstream ss(s);
        std::string item;
        std::vector<std::string> elems;
        while (std::getline(ss, item, delim)) {
            elems.push_back(std::move(item));
        }
        // Special case handling if the last column is empty, we should return empty string
        if (s[s.length()-1] == delim)
            elems.emplace_back("");
        return elems;
    }

    /**
     * This creates a vector for a given data type
     * @param datatype
     * @return shared point of vector
     */
    std::shared_ptr<void> createBuffer(tiledb_datatype_t datatype);

    class Array {
    public:

        ~Array() {
            query.reset(nullptr);
            array.reset(nullptr);
            ctx.reset();
        }

        /**
         * Load a data file into array, this function is generic and works for everything except master data which we should collapse here
         * @param file_uris where data is located
         * @param delimiter of file
         * @param batchSize how many rows to load at once
         * @return  status
         */
        virtual int load(const std::vector<std::string> file_uris, char delimiter, uint64_t batchSize, uint32_t threads);


        virtual void createArray(tiledb::FilterList coordinate_filter_list, tiledb::FilterList offset_filter_list,
                                         tiledb::FilterList attribute_filter_list) = 0;

        /**
         * Parse header is a function for parsing the header row of a file
         * @param headerLine
         * @param delimiter
         * @return vector containing field names in order from file
         */
        virtual std::vector<std::string> parseHeader(std::string headerLine, char delimiter);

        /**
         * Append a string value from the delimited data value to a given buffer
         * @param fieldName
         * @param value
         * @param buffer
         */
        void appendBuffer(const std::string &fieldName, const std::string &value, std::shared_ptr<buffer> buffer);

        /**
         * Templated version used for numeric types
         * @tparam T
         * @param fieldName
         * @param value
         * @param buffer
         */
        template <typename T>
        void appendBuffer(const std::string &fieldName, const T value, std::shared_ptr<buffer> buffer);


        /**
         * Parse a file in parallel to a buffer
         * @param file_uri
         * @param staticColumns
         * @param dimensionFields
         * @param delimiter
         * @param arraySchema
         * @return
         */
        std::unordered_map<std::string, std::shared_ptr<nyse::buffer>> parseFileToBuffer(const std::string &file_uri,
                                                                                         std::unordered_map<std::string, std::string> staticColumns,
                                                                                         std::shared_ptr<std::unordered_map<std::string, std::pair<std::string, std::unordered_map<std::string, std::string>*>>> mapColumns,
                                                                                         std::set<std::string> *dimensionFields,
                                                                                         char delimiter,
                                                                                         tiledb::ArraySchema &arraySchema);

        /**
         * Get tiledb context shared ptr
         * @return
         */
        const std::shared_ptr<tiledb::Context> &getCtx() const;

        //void read(void *subarray);
        virtual uint64_t readSample(std::string outfile, std::string delimiter) = 0;

    protected:
        /**
         * Submit query to tiledb for writing
         * @return status
         */
        tiledb::Query::Status submit_query();

        /**
         * Function to initialize all empty buffers for writting
         * @param headerFields
         * @return
         */
        std::unordered_map<std::string, std::shared_ptr<buffer>> initBuffers(std::vector<std::string> headerFields, std::unordered_map<std::string, std::string> staticColumns);

        std::string array_uri;
        std::unique_ptr<tiledb::Array> array;
        std::unique_ptr<tiledb::Query> query;
        std::shared_ptr<tiledb::Context> ctx;

        // Static columns allows defining a constant value for a given column for all rows, i.e. date.
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> staticColumnsForFiles;
        std::unordered_map<std::string, std::shared_ptr<buffer>> globalBuffers;

        void concatBuffers(std::shared_ptr<void> globalBuffer, std::shared_ptr<void> bufferToAppend, tiledb_datatype_t datatype);

        void concatOffsets(std::shared_ptr<std::vector<uint64_t>> globalOffsets, std::shared_ptr<std::vector<uint64_t>> bufferOffsets, std::shared_ptr<void> values, tiledb_datatype_t datatype);

        uint64_t buffer_size = 10*1024*1024;

        char delimiter;

        FileType type;

        std::shared_timed_mutex mapColumnsMutex;
        std::unordered_map<std::string, std::shared_ptr<std::unordered_map<std::string, std::pair<std::string, std::unordered_map<std::string, std::string>*>>>> mapColumnsForFiles;

    };
}

#endif //NYSE_INGESTOR_ARRAY_H
