/**
 * @file  OpenBook.cc
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

#include "OpenBook.h"
#include <ProgressBar.hpp>
#include <date/date.h>
#include <date/tz.h>
#include <fstream>
#include <netinet/in.h>
#include <tiledb/tiledb>

nyse::OpenBook::OpenBook(std::string array_name, std::string master_file,
                         char delimiter) {
  this->array_uri = std::move(array_name);
  tiledb::Config config;
  config.set("sm.dedup_coords", "true");
  this->ctx = std::make_shared<tiledb::Context>(config);
  this->type_ = FileType::OpenBook;

  this->master_file = master_file;
}

void nyse::OpenBook::createArray(tiledb::FilterList coordinate_filter_list,
                                 tiledb::FilterList offset_filter_list,
                                 tiledb::FilterList attribute_filter_list) {
  // If the array already exists on disk, return immediately.
  if (tiledb::Object::object(*ctx, array_uri).type() ==
      tiledb::Object::Type::Array)
    return;

  tiledb::Domain domain(*ctx);

  // symbol_id
  domain.add_dimension(
      tiledb::Dimension::create<uint64_t>(*ctx, "symbol_id", {{0, 100000}}, 1));

  // time
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(
      *ctx, "datetime", {{0, UINT64_MAX - 60UL * 60 * 1000000000}},
      60UL * 60 * 1000000000));

  // Sequence_Number
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(
      *ctx, "MsgSeqNum", {{0, UINT64_MAX - 1}}, UINT64_MAX));

  // The array will be sparse.
  tiledb::ArraySchema schema(*ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  if (coordinate_filter_list.nfilters() > 0) {
    schema.set_coords_filter_list(coordinate_filter_list);
  }

  if (offset_filter_list.nfilters() > 0) {
    schema.set_offsets_filter_list(offset_filter_list);
  }

  // Set array capacity
  schema.set_capacity(10000000);

  // Set compression filter to ZSTD if not already set
  if (attribute_filter_list.nfilters() == 0) {
    tiledb::Filter compressor(*ctx, TILEDB_FILTER_ZSTD);
    attribute_filter_list.add_filter(compressor);
  }

  tiledb::Attribute MsgType =
      tiledb::Attribute::create<uint16_t>(*ctx, "MsgType")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute SendTime =
      tiledb::Attribute::create<uint32_t>(*ctx, "SendTime")
          .set_filter_list(attribute_filter_list);

  tiledb::Attribute Symbol =
      tiledb::Attribute::create<std::string>(*ctx, "Symbol")
          .set_filter_list(attribute_filter_list);

  tiledb::Attribute MsgSize =
      tiledb::Attribute::create<uint16_t>(*ctx, "MsgSize")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute SecurityIndex =
      tiledb::Attribute::create<uint32_t>(*ctx, "SecurityIndex")
          .set_filter_list(attribute_filter_list);
  /*  tiledb::Attribute SourceTime =
        tiledb::Attribute::create<int32_t>(*ctx, "SourceTime")
            .set_filter_list(attribute_filter_list);
    tiledb::Attribute SourceTimeMicroSecs =
        tiledb::Attribute::create<int16_t>(*ctx, "SourceTimeMicroSecs")
            .set_filter_list(attribute_filter_list);*/
  tiledb::Attribute QuoteCondition =
      tiledb::Attribute::create<uint8_t>(*ctx, "QuoteCondition")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute TradingStatus =
      tiledb::Attribute::create<char>(*ctx, "TradingStatus")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute SourceSeqNum =
      tiledb::Attribute::create<uint32_t>(*ctx, "SourceSeqNum")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute SourceSessionID =
      tiledb::Attribute::create<uint8_t>(*ctx, "SourceSessionID")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute PriceScaleCode =
      tiledb::Attribute::create<uint8_t>(*ctx, "PriceScaleCode")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute PriceNumerator =
      tiledb::Attribute::create<uint32_t>(*ctx, "PriceNumerator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Volume = tiledb::Attribute::create<uint32_t>(*ctx, "Volume")
                                 .set_filter_list(attribute_filter_list);
  tiledb::Attribute ChgQty = tiledb::Attribute::create<uint32_t>(*ctx, "ChgQty")
                                 .set_filter_list(attribute_filter_list);
  tiledb::Attribute NumOrders =
      tiledb::Attribute::create<uint16_t>(*ctx, "NumOrders")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Side = tiledb::Attribute::create<char>(*ctx, "Side")
                               .set_filter_list(attribute_filter_list);
  tiledb::Attribute Filler1 = tiledb::Attribute::create<char>(*ctx, "Filler1")
                                  .set_filter_list(attribute_filter_list);
  tiledb::Attribute ReasonCode =
      tiledb::Attribute::create<char>(*ctx, "ReasonCode")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Filler2 = tiledb::Attribute::create<char>(*ctx, "Filler2")
                                  .set_filter_list(attribute_filter_list);
  tiledb::Attribute LinkID1 =
      tiledb::Attribute::create<uint32_t>(*ctx, "LinkID1")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute LinkID2 =
      tiledb::Attribute::create<uint32_t>(*ctx, "LinkID2")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute LinkID3 =
      tiledb::Attribute::create<uint32_t>(*ctx, "LinkID3")
          .set_filter_list(attribute_filter_list);

  schema.add_attributes(MsgType, SendTime, Symbol, MsgSize, SecurityIndex,
                        QuoteCondition, TradingStatus, SourceSeqNum,
                        SourceSessionID, PriceScaleCode, PriceNumerator, Volume,
                        ChgQty, NumOrders, Side, Filler1, ReasonCode, Filler2,
                        LinkID1, LinkID2, LinkID3);

  // Create the (empty) array on disk.
  tiledb::Array::create(array_uri, schema);
}

int nyse::OpenBook::load(const std::vector<std::string> file_uris,
                         char delimiter, uint64_t batchSize, uint32_t threads) {
  std::unordered_map<std::string, std::string> symbol_lookup =
      nyse::Master::buildSymbolIds(*ctx, master_file, delimiter);
  std::unordered_map<
      std::string,
      std::pair<std::string, std::unordered_map<std::string, std::string> *>>
      mapColumns = {{"symbol_id", {"Symbol", &symbol_lookup}}};
  std::shared_ptr<std::unordered_map<
      std::string,
      std::pair<std::string, std::unordered_map<std::string, std::string> *>>>
      mapColumnsPtr = std::make_shared<std::unordered_map<
          std::string,
          std::pair<std::string,
                    std::unordered_map<std::string, std::string> *>>>(
          mapColumns);

  for (std::string file_uri : file_uris) {
    auto fileSplits = split(file_uri, '_');
    std::unordered_map<std::string, std::string> fileStaticColumns;
    fileStaticColumns.emplace("date", fileSplits.back());
    fileStaticColumns.emplace("datetime", fileSplits.back());
    this->staticColumnsForFiles.emplace(file_uri, fileStaticColumns);

    this->mapColumnsForFiles.emplace(file_uri, mapColumnsPtr);
  }
  // return openbook_load(file_uris, delimiter, batchSize, threads);
  return Array::load(file_uris, delimiter, batchSize, threads);
}

std::unordered_map<std::string, std::shared_ptr<nyse::buffer>>
nyse::OpenBook::parseBinaryFileToBuffer(
    const std::string file_uri,
    std::unordered_map<std::string, std::string> staticColumns,
    std::shared_ptr<std::unordered_map<
        std::string,
        std::pair<std::string, std::unordered_map<std::string, std::string> *>>>
        mapColumns,
    std::set<std::string> *dimensionFields, char delimiter,
    tiledb::ArraySchema &arraySchema) {
  uint64_t totalRowsInFile = 0;
  std::unordered_map<std::string, std::shared_ptr<buffer>> buffers;
  // std::ifstream is(file_uri, std::ifstream::ate | std::ifstream::binary);
  std::ifstream is(file_uri.c_str());
  if (!is.good())
    throw std::runtime_error("Error opening " + file_uri);

  // Get number of lines for progressbar
  std::cout << "Getting number of lines in file: " << file_uri << std::endl;

  is.seekg(0, std::ios::end);
  long length = is.tellg();
  long linesInFile = length / sizeof(fixedLengthFormat);
  is.seekg(0, std::ios::beg);

  // Parse the header row, this is a virtual function because Trade data needs
  // to remove spaces from header columns
  std::vector<std::string> headerFields = {
      "MsgSeqNum",      "MsgType",        "SendTime",     "Symbol",
      "MsgSize",        "SecurityIndex",  "SourceTime",   "SourceTimeMicroSecs",
      "QuoteCondition", "TradingStatus",  "SourceSeqNum", "SourceSessionID",
      "PriceScaleCode", "PriceNumerator", "Volume",       "ChgQty",
      "NumOrders",      "Side",           "Filler1",      "ReasonCode",
      "Filler2",        "LinkID1",        "LinkID2",      "LinkID3"};

  buffers = initBuffers(headerFields, staticColumns);

  std::unordered_map<std::string, tiledb::Attribute> attributes =
      arraySchema.attributes();

  // Check to see if all dimensions are in file being loaded
  for (const std::string &dimension : *dimensionFields) {
    // First check to see if dimension is in static map
    if (staticColumns.find(dimension) != staticColumns.end() ||
        mapColumns->find(dimension) != mapColumns->end() ||
        dimension == "symbol_id" || dimension == "datetime")
      continue;

    bool found = false;
    for (const std::string &field : headerFields) {
      if (field == dimension) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::cerr << "Dimension " << dimension
                << " is not present in data file! Aborting loading"
                << std::endl;
      return buffers;
    }
  }

  uint32_t windowSize = 70;
#ifdef __LINUX__
  struct winsize size;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &size);
  windowSize = size.ws_row;
  std::cout << "window size: " << windowSize << std::endl;
#endif

  // Create progress bar
  ProgressBar progressBar(file_uri, linesInFile - 1, windowSize);

  int rowsParsed = 0;
  // unsigned long expectedFields = dimensionFields.size() /*-
  // staticColumns.size()*/ + arraySchema.attribute_num();
  std::cout << "starting parsing for " << file_uri << " which is "
            << linesInFile << " rows using batch size " << std::endl;
  //<< batchSize << std::endl;

  fixedLengthFormat *rowData = new fixedLengthFormat;
  while (is.tellg() < length) {
    is.read(reinterpret_cast<char *>(rowData), sizeof(fixedLengthFormat));
    if (is.fail())
      break;
    totalRowsInFile++;

    // convert from network to host byte order
    rowData->MsgSeqNum = ntohl(rowData->MsgSeqNum);
    rowData->SendTime = ntohl(rowData->SendTime);
    rowData->MsgSize = ntohs(rowData->MsgSize);
    rowData->SecurityIndex = ntohl(rowData->SecurityIndex);
    rowData->SourceTime = ntohl(rowData->SourceTime);
    rowData->SourceTimeMicroSecs = ntohs(rowData->SourceTimeMicroSecs);
    // rowData->QuoteCondition = ntohs(rowData->QuoteCondition);
    rowData->SourceSeqNum = ntohl(rowData->SourceSeqNum);
    // rowData->SourceSessionID = ntohs(rowData->SourceSessionID);
    // rowData->PriceScaleCode = ntohs(rowData->PriceScaleCode);
    rowData->PriceNumerator = ntohl(rowData->PriceNumerator);
    rowData->Volume = ntohl(rowData->Volume);
    rowData->ChgQty = ntohl(rowData->ChgQty);
    rowData->NumOrders = ntohs(rowData->NumOrders);
    rowData->LinkID1 = ntohl(rowData->LinkID1);
    rowData->LinkID2 = ntohl(rowData->LinkID2);
    rowData->LinkID3 = ntohl(rowData->LinkID3);

    appendBuffer("MsgType", rowData->MsgType, buffers["MsgType"]);
    appendBuffer("SendTime", rowData->SendTime, buffers["SendTime"]);
    std::string symbol = std::string(rowData->Symbol, 11);
    appendBuffer("Symbol", nyse::trim(symbol), buffers["Symbol"]);
    appendBuffer("MsgSize", rowData->MsgSize, buffers["MsgSize"]);
    appendBuffer("SecurityIndex", rowData->SecurityIndex,
                 buffers["SecurityIndex"]);
    appendBuffer("QuoteCondition", rowData->QuoteCondition,
                 buffers["QuoteCondition"]);
    appendBuffer("TradingStatus", rowData->TradingStatus,
                 buffers["TradingStatus"]);
    appendBuffer("SourceSeqNum", rowData->SourceSeqNum,
                 buffers["SourceSeqNum"]);
    appendBuffer("SourceSessionID", rowData->SourceSessionID,
                 buffers["SourceSessionID"]);
    appendBuffer("PriceScaleCode", rowData->PriceScaleCode,
                 buffers["PriceScaleCode"]);
    appendBuffer("PriceNumerator", rowData->PriceNumerator,
                 buffers["PriceNumerator"]);
    appendBuffer("Volume", rowData->Volume, buffers["Volume"]);
    appendBuffer("ChgQty", rowData->ChgQty, buffers["ChgQty"]);
    appendBuffer("NumOrders", rowData->NumOrders, buffers["NumOrders"]);

    appendBuffer("Side", rowData->Side, buffers["Side"]);
    appendBuffer("Filler1", rowData->Filler1, buffers["Filler1"]);
    appendBuffer("ReasonCode", rowData->ReasonCode, buffers["ReasonCode"]);
    appendBuffer("Filler2", &rowData->Filler2, buffers["Filler2"]);
    appendBuffer("LinkID1", rowData->LinkID1, buffers["LinkID1"]);
    appendBuffer("LinkID2", rowData->LinkID2, buffers["LinkID2"]);
    appendBuffer("LinkID3", rowData->LinkID2, buffers["LinkID3"]);

    // Add dimension to coordinate buffer in order
    for (const tiledb::Dimension &dimension :
         arraySchema.domain().dimensions()) {
      int64_t value;
      auto mapColumnsEntry = mapColumns->find(dimension.name());
      if (dimension.name() == "MsgSeqNum") {
        value = rowData->MsgSeqNum;
      } else if (dimension.name() == "symbol_id") {
        std::pair<std::string, std::unordered_map<std::string, std::string> *>
            mapping = mapColumnsEntry->second;
        std::shared_lock<std::shared_timed_mutex> readLock(mapColumnsMutex);
        auto valueMap = mapping.second->find(symbol);
        if (valueMap == mapping.second->end()) {
          readLock.unlock();
          std::lock_guard<std::shared_timed_mutex> writeLock(mapColumnsMutex);
          value = mapping.second->size();
          mapping.second->emplace(symbol, std::to_string(value));
        } else {
          value = std::stoll(valueMap->second);
        }
      } else if (dimension.name() == "datetime") {

        std::string date =
            staticColumnsForFiles.find(file_uri)->second.find("date")->second;
        std::string datetime = date + "-0400";

        date::sys_time<std::chrono::nanoseconds> t;
        std::istringstream stream{datetime};
        stream >> date::parse("%Y%m%d%z", t);
        if (stream.fail())
          throw std::runtime_error("failed to parse " + datetime);

        auto milliseconds = std::chrono::milliseconds(rowData->SourceTime);
        auto microseconds =
            std::chrono::microseconds(rowData->SourceTimeMicroSecs);
        auto finalTime = t + milliseconds + microseconds;

        auto UTC = date::make_zoned("GMT", finalTime).get_local_time();
        value = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    UTC.time_since_epoch())
                    .count();
      } else {
        auto staticColumnsForFile = staticColumnsForFiles.find(file_uri);
        if (staticColumnsForFile == staticColumnsForFiles.end()) {
          std::cout << "Warning " << file_uri
                    << " was missing static column mapping for row "
                    << totalRowsInFile << ". Aborting!!" << std::endl;
          return buffers;
        }

        value = std::stoll(staticColumnsForFiles.find(file_uri)
                               ->second.find(dimension.name())
                               ->second);
      }
      appendBuffer(dimension.name(), value,
                   buffers.find(TILEDB_COORDS)->second);
    }

    rowsParsed++;
    ++progressBar;
    progressBar.display();
  }
  delete rowData;
  progressBar.done();
  is.close();
  return buffers;
}

/*
int nyse::OpenBook::openbook_load(std::vector<std::string> file_uris, char
delimiter, uint64_t batchSize, uint32_t threads) { unsigned long totalRows = 0;

  ThreadPool pool(threads);
  std::vector<
          std::future<std::unordered_map<std::string, std::shared_ptr<buffer>>>>
          results;

  array = std::make_unique<tiledb::Array>(*ctx, array_uri,
                                          tiledb_query_type_t::TILEDB_WRITE);
  query = std::make_unique<tiledb::Query>(*ctx, *array);

  query->set_layout(tiledb_layout_t::TILEDB_UNORDERED);

  tiledb::ArraySchema arraySchema = array->schema();

  std::set<std::string> dimensionFields;
  for (const tiledb::Dimension &dimension : arraySchema.domain().dimensions()) {
    dimensionFields.emplace(dimension.name());
  }

  auto startTime = std::chrono::steady_clock::now();

  for (const std::string &file_uri : file_uris) {
    if (staticColumnsForFiles.find(file_uri) == staticColumnsForFiles.end()) {
      std::cerr << "File " << file_uri
                << " missing static columns mapping!! Aborting!!" << std::endl;
      return -1;
    }
    std::unordered_map<std::string, std::string> staticColumns =
            staticColumnsForFiles.find(file_uri)->second;
    std::shared_ptr<std::unordered_map<
            std::string,
            std::pair<std::string, std::unordered_map<std::string, std::string>
*>>> mapColumns = nullptr; auto mapColumnsForFilesEntry =
mapColumnsForFiles.find(file_uri); if (mapColumnsForFilesEntry !=
mapColumnsForFiles.end()) mapColumns = mapColumnsForFilesEntry->second; else
      mapColumns = std::make_shared<std::unordered_map<
              std::string,
              std::pair<std::string,
                      std::unordered_map<std::string, std::string> *>>>();
    results.push_back(pool.enqueue(staticLoadJob, this, file_uri, staticColumns,
                                   mapColumns, &dimensionFields, delimiter,
                                   arraySchema));
  }

  for (auto &result : results) {
    auto buffers = result.get();
    for (auto entry : buffers) {
      std::string bufferName = entry.first;
      if (bufferName == TILEDB_COORDS) {
        totalRows += std::static_pointer_cast<std::vector<uint64_t>>(
                entry.second->values)
                             ->size() /
                     dimensionFields.size();
      }
      auto globalBuffer = globalBuffers.find(bufferName);
      if (globalBuffer != globalBuffers.end()) {
        if (entry.second->offsets != nullptr) {
          concatOffsets(globalBuffer->second->offsets, entry.second->offsets,
                        globalBuffer->second->values, entry.second->datatype);
          entry.second->offsets.reset();
        }
        if (entry.second->values != nullptr) {
          concatBuffers(globalBuffer->second->values, entry.second->values,
                        entry.second->datatype);
          entry.second->values.reset();
        }
      } else {
        globalBuffers.emplace(bufferName, entry.second);
      }
    }
  }

  if (submit_query() == tiledb::Query::Status::FAILED) {
    std::cerr << "Query FAILED!!!!!" << std::endl;
  }

  query->finalize();

  array->close();

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now() - startTime);
  printf("loaded %ld rows in %s (%.2f rows/second)\n", totalRows,
         beautify_duration(duration).c_str(),
         (float(totalRows)) / duration.count());

  return 0;
}*/

uint64_t nyse::OpenBook::readSample(std::string outfile,
                                    std::string delimiter) {
  uint64_t rows_read = 0;
  array = std::make_unique<tiledb::Array>(*ctx, array_uri,
                                          tiledb_query_type_t::TILEDB_READ);
  query = std::make_unique<tiledb::Query>(*ctx, *array);

  std::ofstream output;
  if (!outfile.empty()) {
    output = std::ofstream(outfile);
  }

  query->set_layout(tiledb_layout_t::TILEDB_GLOBAL_ORDER);

  tiledb::ArraySchema arraySchema = array->schema();

  auto nonEmptyDomain = array->non_empty_domain<uint64_t>();

  // 2018-07-30 09:30:00.000 to 2018-07-30 12:30:00.000
  // Then select the entire domain for sequence number
  // std::vector<uint64_t> subarray = {1532957400000000000, 1532968200000000000,
  // nonEmptyDomain[1].second.first, nonEmptyDomain[1].second.second};
  std::vector<uint64_t> subarray = {
      nonEmptyDomain[0].second.first, nonEmptyDomain[0].second.second,
      nonEmptyDomain[1].second.first, nonEmptyDomain[1].second.second};
  query->set_subarray(subarray);

  std::vector<uint64_t> coords(this->buffer_size / sizeof(uint64_t));
  query->set_coordinates(coords);

  std::vector<char> Exchange(this->buffer_size / sizeof(char));
  query->set_buffer("Exchange", Exchange);

  std::vector<char> Symbol(this->buffer_size / sizeof(char));
  std::vector<uint64_t> Symbol_offsets(this->buffer_size / sizeof(uint64_t));
  query->set_buffer("Symbol", Symbol_offsets, Symbol);

  std::vector<float> Bid_Price(this->buffer_size / sizeof(float));
  query->set_buffer("Bid_Price", Bid_Price);

  std::vector<uint32_t> Bid_Size(this->buffer_size / sizeof(uint32_t));
  query->set_buffer("Bid_Size", Bid_Size);

  std::vector<float> Offer_Price(this->buffer_size / sizeof(float));
  query->set_buffer("Offer_Price", Offer_Price);

  std::vector<uint32_t> Offer_Size(this->buffer_size / sizeof(uint32_t));
  query->set_buffer("Offer_Size", Offer_Size);

  std::vector<char> OpenBook_Condition(this->buffer_size / sizeof(char));
  query->set_buffer("OpenBook_Condition", OpenBook_Condition);

  std::vector<char> National_BBO_Ind(this->buffer_size / sizeof(char));
  query->set_buffer("National_BBO_Ind", National_BBO_Ind);

  std::vector<char> FINRA_BBO_Indicator(this->buffer_size / sizeof(char));
  query->set_buffer("FINRA_BBO_Indicator", FINRA_BBO_Indicator);

  std::vector<uint8_t> FINRA_ADF_MPID_Indicator(this->buffer_size /
                                                sizeof(uint8_t));
  query->set_buffer("FINRA_ADF_MPID_Indicator", FINRA_ADF_MPID_Indicator);

  std::vector<char> OpenBook_Cancel_Correction(this->buffer_size /
                                               sizeof(char));
  query->set_buffer("OpenBook_Cancel_Correction", OpenBook_Cancel_Correction);

  std::vector<char> Source_Of_OpenBook(this->buffer_size / sizeof(char));
  query->set_buffer("Source_Of_OpenBook", Source_Of_OpenBook);

  std::vector<char> Retail_Interest_Indicator(this->buffer_size / sizeof(char));
  query->set_buffer("Retail_Interest_Indicator", Retail_Interest_Indicator);

  std::vector<char> Short_Sale_Restriction_Indicator(this->buffer_size /
                                                     sizeof(char));
  query->set_buffer("Short_Sale_Restriction_Indicator",
                    Short_Sale_Restriction_Indicator);

  std::vector<char> LULD_BBO_Indicator(this->buffer_size / sizeof(char));
  query->set_buffer("LULD_BBO_Indicator", LULD_BBO_Indicator);

  std::vector<char> SIP_Generated_Message_Identifier(this->buffer_size /
                                                     sizeof(char));
  query->set_buffer("SIP_Generated_Message_Identifier",
                    SIP_Generated_Message_Identifier);

  std::vector<char> National_BBO_LULD_Indicator(this->buffer_size /
                                                sizeof(char));
  query->set_buffer("National_BBO_LULD_Indicator", National_BBO_LULD_Indicator);

  std::vector<uint64_t> Participant_Timestamp(this->buffer_size /
                                              sizeof(uint64_t));
  query->set_buffer("Participant_Timestamp", Participant_Timestamp);

  std::vector<uint64_t> FINRA_ADF_Timestamp(this->buffer_size /
                                            sizeof(uint64_t));
  query->set_buffer("FINRA_ADF_Timestamp", FINRA_ADF_Timestamp);

  std::vector<char> FINRA_ADF_Market_Participant_OpenBook_Indicator(
      this->buffer_size / sizeof(char));
  query->set_buffer("FINRA_ADF_Market_Participant_OpenBook_Indicator",
                    FINRA_ADF_Market_Participant_OpenBook_Indicator);

  std::vector<char> Security_Status_Indicator(this->buffer_size / sizeof(char));
  query->set_buffer("Security_Status_Indicator", Security_Status_Indicator);

  tiledb::Query::Status status;
  size_t previous_result_num = 0;
  do {
    // Submit query and get status
    query->submit();
    status = query->query_status();

    // If any results were retrieved, parse and print them
    auto result_num =
        (int)query->result_buffer_elements()[TILEDB_COORDS].second /
        arraySchema.domain().ndim();
    rows_read += result_num;
    if (status == tiledb::Query::Status::INCOMPLETE &&
        result_num == 0) { // VERY IMPORTANT!!
      std::cerr << "Buffers were too small for query, you should fix this, "
                   "test is invalid"
                << std::endl;
      std::cerr << "Last complete rows read: " << rows_read << std::endl;
      std::cerr << "Last good coordinates [" << coords[previous_result_num - 2]
                << ", " << coords[previous_result_num - 1]
                << "] for result size " << previous_result_num << std::endl;
      break;
      // reallocate_buffers(&coords, &a1_data, &a2_off, &a2_data);
    }
    if (output.is_open()) {
      for (int i = 0; i < result_num; i++) {
        std::stringstream ss;
        ss << std::to_string(coords[i * 2]);
        ss << delimiter;
        ss << std::to_string(coords[i * 2 + 1]);
        ss << delimiter;
        ss << std::string(1, Exchange[i]);
        ss << delimiter;
        size_t symbolEnd =
            ((i + 1 == result_num) ? Symbol.size() : Symbol_offsets[i + 1]);
        ss << std::string(Symbol.begin() + Symbol_offsets[i],
                          Symbol.begin() + symbolEnd);
        ss << delimiter;
        ss << std::to_string(Bid_Price[i]);
        ss << delimiter;
        ss << std::to_string(Bid_Size[i]);
        ss << delimiter;
        ss << std::to_string(Offer_Price[i]);
        ss << delimiter;
        ss << std::to_string(Offer_Size[i]);
        ss << delimiter;
        ss << std::string(1, OpenBook_Condition[i]);
        ss << delimiter;
        ss << std::string(1, National_BBO_Ind[i]);
        ss << delimiter;
        ss << std::string(1, FINRA_BBO_Indicator[i]);
        ss << delimiter;
        ss << std::to_string(FINRA_ADF_MPID_Indicator[i]);
        ss << delimiter;
        ss << std::string(1, OpenBook_Cancel_Correction[i]);
        ss << delimiter;
        ss << std::string(1, Source_Of_OpenBook[i]);
        ss << delimiter;
        ss << std::string(1, Retail_Interest_Indicator[i]);
        ss << delimiter;
        ss << std::string(1, Short_Sale_Restriction_Indicator[i]);
        ss << delimiter;
        ss << std::string(1, LULD_BBO_Indicator[i]);
        ss << delimiter;
        ss << std::string(1, SIP_Generated_Message_Identifier[i]);
        ss << delimiter;
        ss << std::string(1, National_BBO_LULD_Indicator[i]);
        ss << delimiter;
        ss << std::to_string(Participant_Timestamp[i]);
        ss << delimiter;
        ss << std::to_string(FINRA_ADF_Timestamp[i]);
        ss << delimiter;
        ss << std::string(1,
                          FINRA_ADF_Market_Participant_OpenBook_Indicator[i]);
        ss << delimiter;
        ss << std::string(1, Security_Status_Indicator[i]);
        ss << std::endl;

        output << ss.rdbuf();
      }
    }

    // std::cerr << "Last good coordinates [" << coords[result_num-2] << ", " <<
    // coords[result_num-1] << "] for result size " << result_num << std::endl;
    previous_result_num = result_num;
  } while (status == tiledb::Query::Status::INCOMPLETE);

  if (output.is_open()) {
    output.close();
  }

  return rows_read;
}
