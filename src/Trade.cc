/**
 * @file  Trade.cc
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
 * Represents the trade data file for NYSE data
 *
 */

#include "Trade.h"
#include <fstream>
#include <tiledb/tiledb>

nyse::Trade::Trade(std::string array_name, std::string master_file,
                   char delimiter) {
  this->array_uri = std::move(array_name);
  tiledb::Config config;
  config.set("sm.dedup_coords", "true");
  this->ctx = std::make_shared<tiledb::Context>(config);
  this->type_ = FileType::Trade;

  this->master_file = master_file;
}

std::vector<std::string> nyse::Trade::parseHeader(std::string headerLine,
                                                  char delimiter) {
  std::vector<std::string> headerColumns;
  for (std::string field : split(headerLine, delimiter)) {
    std::replace(field.begin(), field.end(), ' ', '_');
    headerColumns.push_back(field);
  }
  return headerColumns;
}

void nyse::Trade::createArray(tiledb::FilterList coordinate_filter_list,
                              tiledb::FilterList offset_filter_list,
                              tiledb::FilterList attribute_filter_list) {
  // If the array already exists on disk, return immediately.
  if (tiledb::Object::object(*ctx, array_uri).type() ==
      tiledb::Object::Type::Array)
    return;

  tiledb::Domain domain(*ctx);
  // symbol_id
  domain.add_dimension(
      tiledb::Dimension::create<uint64_t>(*ctx, "symbol_id", {{0, 10000}}, 1));

  // time
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(
      *ctx, "datetime", {{0, UINT64_MAX - 60 * 60 * 1000000000UL}},
      60 * 60UL * 1000000000));

  // Sequence_Number
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(
      *ctx, "Sequence_Number", {{0, UINT64_MAX - 1}}, UINT64_MAX));

  // The array will be dense.
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

  tiledb::Attribute Exchange = tiledb::Attribute::create<char>(*ctx, "Exchange")
                                   .set_filter_list(attribute_filter_list);
  tiledb::Attribute Symbol =
      tiledb::Attribute::create<std::string>(*ctx, "Symbol")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Sale_Condition =
      tiledb::Attribute::create<std::string>(*ctx, "Sale_Condition")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Volume =
      tiledb::Attribute::create<uint32_t>(*ctx, "Trade_Volume")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Price =
      tiledb::Attribute::create<float>(*ctx, "Trade_Price")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Stop_Stock_Indicator =
      tiledb::Attribute::create<char>(*ctx, "Trade_Stop_Stock_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Correction_Indicator =
      tiledb::Attribute::create<uint8_t>(*ctx, "Trade_Correction_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Id =
      tiledb::Attribute::create<std::string>(*ctx, "Trade_Id")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Source_of_Trade =
      tiledb::Attribute::create<char>(*ctx, "Source_of_Trade")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Reporting_Facility =
      tiledb::Attribute::create<char>(*ctx, "Trade_Reporting_Facility")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Participant_Timestamp =
      tiledb::Attribute::create<uint64_t>(*ctx, "Participant_Timestamp")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Reporting_Facility_TRF_Timestamp =
      tiledb::Attribute::create<uint64_t>(
          *ctx, "Trade_Reporting_Facility_TRF_Timestamp")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Trade_Through_Exempt_Indicator =
      tiledb::Attribute::create<uint8_t>(*ctx, "Trade_Through_Exempt_Indicator")
          .set_filter_list(attribute_filter_list);

  schema.add_attributes(
      Exchange, Symbol, Sale_Condition, Trade_Volume, Trade_Price,
      Trade_Stop_Stock_Indicator, Trade_Correction_Indicator, Trade_Id,
      Source_of_Trade, Trade_Reporting_Facility, Participant_Timestamp,
      Trade_Reporting_Facility_TRF_Timestamp, Trade_Through_Exempt_Indicator);

  // Create the (empty) array on disk.
  tiledb::Array::create(array_uri, schema);
}

int nyse::Trade::load(const std::vector<std::string> file_uris, char delimiter,
                      uint64_t batchSize, uint32_t threads) {
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
  return Array::load(file_uris, delimiter, batchSize, threads);
}

uint64_t nyse::Trade::readSample(std::string outfile, std::string delimiter) {
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

  // Then select the entire domain for sequence number
  std::vector<uint64_t> subarray = {
      nonEmptyDomain[0].second.first, nonEmptyDomain[0].second.second,
      nonEmptyDomain[1].second.first, nonEmptyDomain[1].second.second};

  query->set_subarray(subarray);

  std::vector<uint64_t> coords(this->buffer_size / sizeof(uint64_t));
  query->set_coordinates(coords);

  auto max_buffer_elements = array->max_buffer_elements(subarray);

  std::vector<char> Exchange(this->buffer_size / sizeof(char));
  query->set_buffer("Exchange", Exchange);

  std::vector<char> Symbol(this->buffer_size / sizeof(char));
  std::vector<uint64_t> Symbol_offsets(this->buffer_size / sizeof(uint64_t));
  query->set_buffer("Symbol", Symbol_offsets, Symbol);

  std::vector<char> Sale_Condition(this->buffer_size / sizeof(char));
  std::vector<uint64_t> Sale_Condition_offsets(this->buffer_size /
                                               sizeof(uint64_t));
  query->set_buffer("Sale_Condition", Sale_Condition_offsets, Sale_Condition);

  std::vector<uint32_t> Trade_Volume(this->buffer_size / sizeof(uint32_t));
  query->set_buffer("Trade_Volume", Trade_Volume);

  std::vector<float> Trade_Price(this->buffer_size / sizeof(float));
  query->set_buffer("Trade_Price", Trade_Price);

  std::vector<char> Trade_Stop_Stock_Indicator(this->buffer_size /
                                               sizeof(char));
  query->set_buffer("Trade_Stop_Stock_Indicator", Trade_Stop_Stock_Indicator);

  std::vector<uint8_t> Trade_Correction_Indicator(this->buffer_size /
                                                  sizeof(uint8_t));
  query->set_buffer("Trade_Correction_Indicator", Trade_Correction_Indicator);

  std::vector<char> Trade_Id(this->buffer_size / sizeof(char));
  std::vector<uint64_t> Trade_Id_offsets(this->buffer_size / sizeof(uint64_t));
  query->set_buffer("Trade_Id", Trade_Id_offsets, Trade_Id);

  std::vector<char> Source_of_Trade(this->buffer_size / sizeof(char));
  query->set_buffer("Source_of_Trade", Source_of_Trade);

  std::vector<char> Trade_Reporting_Facility(this->buffer_size / sizeof(char));
  query->set_buffer("Trade_Reporting_Facility", Trade_Reporting_Facility);

  std::vector<uint64_t> Participant_Timestamp(this->buffer_size /
                                              sizeof(uint64_t));
  query->set_buffer("Participant_Timestamp", Participant_Timestamp);

  std::vector<uint64_t> Trade_Reporting_Facility_TRF_Timestamp(
      this->buffer_size / sizeof(uint64_t));
  query->set_buffer("Trade_Reporting_Facility_TRF_Timestamp",
                    Trade_Reporting_Facility_TRF_Timestamp);

  std::vector<uint8_t> Trade_Through_Exempt_Indicator(this->buffer_size /
                                                      sizeof(uint8_t));
  query->set_buffer("Trade_Through_Exempt_Indicator",
                    Trade_Through_Exempt_Indicator);

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
        size_t symbolLength =
            ((i + 1 == result_num) ? Symbol.size() : Symbol_offsets[i + 1]);
        ss << std::string(Symbol.begin() + Symbol_offsets[i],
                          Symbol.begin() + symbolLength);
        ss << delimiter;
        ss << std::string(1, Sale_Condition[i]);
        ss << delimiter;
        ss << std::to_string(Trade_Volume[i]);
        ss << delimiter;
        ss << std::to_string(Trade_Price[i]);
        ss << delimiter;
        ss << std::string(1, Trade_Stop_Stock_Indicator[i]);
        ss << delimiter;
        ss << std::to_string(Trade_Correction_Indicator[i]);
        ss << delimiter;
        size_t Trade_Id_Length =
            ((i + 1 == result_num) ? Trade_Id.size() : Trade_Id_offsets[i + 1]);
        ss << std::string(Trade_Id.begin() + Trade_Id_offsets[i],
                          Trade_Id.begin() + Trade_Id_Length);
        ss << delimiter;
        ss << std::string(1, Source_of_Trade[i]);
        ss << delimiter;
        ss << std::string(1, Trade_Reporting_Facility[i]);
        ss << delimiter;
        ss << std::to_string(Participant_Timestamp[i]);
        ss << delimiter;
        ss << std::to_string(Trade_Reporting_Facility_TRF_Timestamp[i]);
        ss << delimiter;
        ss << std::to_string(Trade_Through_Exempt_Indicator[i]);
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
