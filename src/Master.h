/**
 * @file  Master.h
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
 * Represents the master data file for NYSE data
 *
 */

#ifndef NYSE_INGESTOR_MASTER_H
#define NYSE_INGESTOR_MASTER_H

#include "Array.h"
#include "buffer.h"
#include <string>

namespace nyse {
class Master : public Array {
public:
  Master(std::string array_name, char delimiter);

  /**
   * Create master symbol array
   */
  void createArray(tiledb::FilterList coordinate_filter_list,
                   tiledb::FilterList offset_filter_list,
                   tiledb::FilterList attribute_filter_list) override;

  /**
   * Load master symbol data into array
   * @param file_uris uri where file is located
   * @param delimiter delimiter of file
   * @param batchSize how many rows to load at once
   * @return status
   */
  int load(const std::vector<std::string> file_uris, char delimiter,
           uint64_t batchSize, uint32_t threads) override;

  uint64_t readSample(std::string outfile, std::string delimiter) { return 0; };

  static std::unordered_map<std::string, std::string>
  buildSymbolIds(tiledb::Context ctx, const std::string &master_file,
                 const char &delimiter);
};
} // namespace nyse

#endif // NYSE_INGESTOR_MASTER_H
