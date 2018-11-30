/**
 * @file  Quote.cc
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
 * Represents the quote data file for NYSE data
 *
 */

#include "Quote.h"
#include <fstream>
#include <tiledb/tiledb>

nyse::Quote::Quote(std::string array_name, std::string master_file,
                   char delimiter) {
  this->array_uri = std::move(array_name);
  tiledb::Config config;
  config.set("sm.dedup_coords", "true");
  this->ctx = std::make_shared<tiledb::Context>(config);
  this->type = FileType::Quote;

  this->master_file = master_file;
}

void nyse::Quote::createArray(tiledb::FilterList coordinate_filter_list,
                              tiledb::FilterList offset_filter_list,
                              tiledb::FilterList attribute_filter_list) {
  // If the array already exists on disk, return immediately.
  if (tiledb::Object::object(*ctx, array_uri).type() ==
      tiledb::Object::Type::Array)
    return;

  tiledb::Domain domain(*ctx);

  // symbol_id
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "symbol_id",
                                                           {{0, 10000}}, 100));

  // time
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(
      *ctx, "datetime", {{0, UINT64_MAX - 60UL * 60 * 1000000000}},
      60UL * 60 * 1000000000));

  // Sequence_Number
  domain.add_dimension(tiledb::Dimension::create<uint64_t>(
      *ctx, "Sequence_Number", {{0, UINT64_MAX - 1}}, UINT64_MAX));

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

  tiledb::Attribute Exchange = tiledb::Attribute::create<char>(*ctx, "Exchange")
                                   .set_filter_list(attribute_filter_list);
  tiledb::Attribute symbol =
      tiledb::Attribute::create<std::string>(*ctx, "Symbol")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Bid_Price =
      tiledb::Attribute::create<float>(*ctx, "Bid_Price")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Bid_Size =
      tiledb::Attribute::create<uint32_t>(*ctx, "Bid_Size")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Offer_Price =
      tiledb::Attribute::create<float>(*ctx, "Offer_Price")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Offer_Size =
      tiledb::Attribute::create<uint32_t>(*ctx, "Offer_Size")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Quote_Condition =
      tiledb::Attribute::create<char>(*ctx, "Quote_Condition")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute National_BBO_Ind =
      tiledb::Attribute::create<char>(*ctx, "National_BBO_Ind")
          .set_filter_list(
              attribute_filter_list); // This is listed as numeric but it is
                                      // alphanumeric (but only a single char)
  tiledb::Attribute FINRA_BBO_Indicator =
      tiledb::Attribute::create<char>(*ctx, "FINRA_BBO_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute FINRA_ADF_MPID_Indicator =
      tiledb::Attribute::create<uint8_t>(*ctx, "FINRA_ADF_MPID_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Quote_Cancel_Correction =
      tiledb::Attribute::create<char>(*ctx, "Quote_Cancel_Correction")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Source_Of_Quote =
      tiledb::Attribute::create<char>(*ctx, "Source_Of_Quote")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Retail_Interest_Indicator =
      tiledb::Attribute::create<char>(*ctx, "Retail_Interest_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Short_Sale_Restriction_Indicator =
      tiledb::Attribute::create<char>(*ctx, "Short_Sale_Restriction_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute LULD_BBO_Indicator =
      tiledb::Attribute::create<char>(*ctx, "LULD_BBO_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute SIP_Generated_Message_Identifier =
      tiledb::Attribute::create<char>(*ctx, "SIP_Generated_Message_Identifier")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute National_BBO_LULD_Indicator =
      tiledb::Attribute::create<char>(*ctx, "National_BBO_LULD_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Participant_Timestamp =
      tiledb::Attribute::create<uint64_t>(*ctx, "Participant_Timestamp")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute FINRA_ADF_Timestamp =
      tiledb::Attribute::create<uint64_t>(*ctx, "FINRA_ADF_Timestamp")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute FINRA_ADF_Market_Participant_Quote_Indicator =
      tiledb::Attribute::create<char>(
          *ctx, "FINRA_ADF_Market_Participant_Quote_Indicator")
          .set_filter_list(attribute_filter_list);
  tiledb::Attribute Security_Status_Indicator =
      tiledb::Attribute::create<char>(*ctx, "Security_Status_Indicator")
          .set_filter_list(attribute_filter_list);

  schema.add_attributes(
      Exchange, symbol, Bid_Price, Bid_Size, Offer_Price, Offer_Size,
      Quote_Condition, National_BBO_Ind, FINRA_BBO_Indicator,
      FINRA_ADF_MPID_Indicator, Quote_Cancel_Correction, Source_Of_Quote,
      Retail_Interest_Indicator, Short_Sale_Restriction_Indicator,
      LULD_BBO_Indicator, SIP_Generated_Message_Identifier,
      National_BBO_LULD_Indicator, Participant_Timestamp, FINRA_ADF_Timestamp,
      FINRA_ADF_Market_Participant_Quote_Indicator, Security_Status_Indicator);

  // Create the (empty) array on disk.
  tiledb::Array::create(array_uri, schema);
}

int nyse::Quote::load(const std::vector<std::string> file_uris, char delimiter,
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

uint64_t nyse::Quote::readSample(std::string outfile, std::string delimiter) {
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

  std::vector<char> Quote_Condition(this->buffer_size / sizeof(char));
  query->set_buffer("Quote_Condition", Quote_Condition);

  std::vector<char> National_BBO_Ind(this->buffer_size / sizeof(char));
  query->set_buffer("National_BBO_Ind", National_BBO_Ind);

  std::vector<char> FINRA_BBO_Indicator(this->buffer_size / sizeof(char));
  query->set_buffer("FINRA_BBO_Indicator", FINRA_BBO_Indicator);

  std::vector<uint8_t> FINRA_ADF_MPID_Indicator(this->buffer_size /
                                                sizeof(uint8_t));
  query->set_buffer("FINRA_ADF_MPID_Indicator", FINRA_ADF_MPID_Indicator);

  std::vector<char> Quote_Cancel_Correction(this->buffer_size / sizeof(char));
  query->set_buffer("Quote_Cancel_Correction", Quote_Cancel_Correction);

  std::vector<char> Source_Of_Quote(this->buffer_size / sizeof(char));
  query->set_buffer("Source_Of_Quote", Source_Of_Quote);

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

  std::vector<char> FINRA_ADF_Market_Participant_Quote_Indicator(
      this->buffer_size / sizeof(char));
  query->set_buffer("FINRA_ADF_Market_Participant_Quote_Indicator",
                    FINRA_ADF_Market_Participant_Quote_Indicator);

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
        ss << std::string(1, Quote_Condition[i]);
        ss << delimiter;
        ss << std::string(1, National_BBO_Ind[i]);
        ss << delimiter;
        ss << std::string(1, FINRA_BBO_Indicator[i]);
        ss << delimiter;
        ss << std::to_string(FINRA_ADF_MPID_Indicator[i]);
        ss << delimiter;
        ss << std::string(1, Quote_Cancel_Correction[i]);
        ss << delimiter;
        ss << std::string(1, Source_Of_Quote[i]);
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
        ss << std::string(1, FINRA_ADF_Market_Participant_Quote_Indicator[i]);
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
