/**
 * @file  Trader.h
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
 * Represents the trader data file for NYSE data
 *
 */

#ifndef NYSE_INGESTOR_TRADE_H
#define NYSE_INGESTOR_TRADE_H


#include <string>
#include "Array.h"

namespace nyse {
    class Trade : public Array {
    public:
        Trade(std::string array_name);

        /**
         * Create trade array
         */
        void createArray(tiledb::FilterList coordinate_filter_list, tiledb::FilterList offset_filter_list,
                         tiledb::FilterList attribute_filter_list) override;

        /**
         * Parse header is a function for parsing the header row of a file, we remove spaces and replace with '_'
         * @param headerLine
         * @param delimiter
         * @return vector containing field names in order from file
         */
        std::vector<std::string> parserHeader(std::string headerLine, char delimiter) override;

        /**
         * Load trade data into array
         * @param file_uris uri where file is located
         * @param delimiter delimiter of file
         * @param batchSize how many rows to load at once
         * @return status
         */
        int load(const std::vector<std::string> file_uris, char delimiter, uint64_t batchSize, uint32_t threads) override;

        uint64_t readSample(std::string outfile, std::string delimiter);
    };
}


#endif //NYSE_INGESTOR_TRADE_H
