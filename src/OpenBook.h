/**
 * @file  OpenBook.h
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
 * Represents the openbook data file for NYSE data
 *
 */

#ifndef NYSE_INGESTOR_OPENBOOK_H
#define NYSE_INGESTOR_OPENBOOK_H

#include "Array.h"
#include "Master.h"
#include <string>

namespace nyse {
class OpenBook : public Array {
public:
#pragma pack(1)
  struct fixedLengthFormat {
    int32_t MsgSeqNum;
    int16_t MsgType;
    int32_t SendTime;
    char Symbol[11];
    int16_t MsgSize;
    int32_t SecurityIndex;
    int32_t SourceTime;
    int16_t SourceTimeMicroSecs;
    int8_t QuoteCondition;
    char TradingStatus;
    int32_t SourceSeqNum;
    int8_t SourceSessionID;
    int8_t PriceScaleCode;
    int32_t PriceNumerator;
    int32_t Volume;
    int32_t ChgQty;
    int16_t NumOrders;
    char Side;
    char Filler1;
    char ReasonCode;
    char Filler2;
    int32_t LinkID1;
    int32_t LinkID2;
    int32_t LinkID3;
  };

  OpenBook(std::string array_name, std::string master_file, char delimiter);

  /**
   * Create openbook array
   */
  void createArray(tiledb::FilterList coordinate_filter_list,
                   tiledb::FilterList offset_filter_list,
                   tiledb::FilterList attribute_filter_list) override;

  /**
   * Load openbook data into array
   * @param file_uris uri where file is located
   * @param delimiter delimiter of file
   * @param batchSize how many rows to load at once
   * @return status
   */
  int load(std::vector<std::string> file_uris, char delimiter,
           uint64_t batchSize, uint32_t threads) override;

  uint64_t readSample(std::string outfile, std::string delimiter) override;

  /**
   * Parse a file in parallel to a buffer
   * @param file_uri
   * @param staticColumns
   * @param dimensionFields
   * @param delimiter
   * @param arraySchema
   * @return
   */
  std::unordered_map<std::string, std::shared_ptr<nyse::buffer>>
  parseBinaryFileToBuffer(
      const std::string file_uri,
      std::unordered_map<std::string, std::string> staticColumns,
      std::shared_ptr<std::unordered_map<
          std::string, std::pair<std::string, std::unordered_map<
                                                  std::string, std::string> *>>>
          mapColumns,
      std::set<std::string> *dimensionFields, char delimiter,
      tiledb::ArraySchema &arraySchema);

  std::string master_file;
  /*private:
          int openbook_load(std::vector<std::string> file_uris, char delimiter,
     uint64_t batchSize, uint32_t threads);*/
};
} // namespace nyse

#endif // NYSE_INGESTOR_OPENBOOK_H
