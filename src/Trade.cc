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
#include <tiledb/tiledb>

nyse::Trade::Trade(std::string array_name) {
    this->array_uri = std::move(array_name);
    tiledb::Config config;
    config.set("sm.dedup_coords", "true");
    this->ctx = std::make_shared<tiledb::Context>(config);
}

std::vector<std::string> nyse::Trade::parserHeader(std::string headerLine, char delimiter) {
    std::vector<std::string> headerColumns;
    for (std::string field : split(headerLine, delimiter)) {
        std::replace(field.begin(), field.end(), ' ', '_');
        headerColumns.push_back(field);
    }
    return headerColumns;
}

void nyse::Trade::createArray(tiledb::FilterList coordinate_filter_list, tiledb::FilterList offset_filter_list,
                              tiledb::FilterList attribute_filter_list) {
    // If the array already exists on disk, return immediately.
    if (tiledb::Object::object(*ctx, array_uri).type() == tiledb::Object::Type::Array)
        return;

    tiledb::Domain domain(*ctx);
    // time
    domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "datetime", {{0, UINT64_MAX - 24UL*60*60*1000000000}}, 24UL*60*60*1000000000));

    // Store up to 2 years of data in array
    //domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "date", {{1, 20381231}}, 31));

    // Nanoseconds since midnight
    //domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "Time", {{0UL, UINT64_MAX - 1}}, 1000000000UL * 60)); // HHMMSSXXXXXXXXX

    // Sequence_Number
    domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "Sequence_Number", {{0, UINT64_MAX - 1}}, UINT64_MAX));

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
        //f.set_option(TILEDB_COMPRESSION_LEVEL, 5);
        attribute_filter_list.add_filter(compressor);
    }

    // Add a single attribute "a" so each (i,j) cell can store an integer.

    tiledb::Attribute Exchange = tiledb::Attribute::create<char>(*ctx, "Exchange").set_filter_list(attribute_filter_list);
    tiledb::Attribute Symbol = tiledb::Attribute::create<std::string>(*ctx, "Symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute Sale_Condition = tiledb::Attribute::create<std::string>(*ctx, "Sale_Condition").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Volume = tiledb::Attribute::create<uint32_t>(*ctx, "Trade_Volume").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Price = tiledb::Attribute::create<double>(*ctx, "Trade_Price").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Stop_Stock_Indicator = tiledb::Attribute::create<char>(*ctx, "Trade_Stop_Stock_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Correction_Indicator = tiledb::Attribute::create<uint8_t >(*ctx, "Trade_Correction_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Id = tiledb::Attribute::create<std::string>(*ctx, "Trade_Id").set_filter_list(attribute_filter_list);
    tiledb::Attribute Source_of_Trade = tiledb::Attribute::create<char>(*ctx, "Source_of_Trade").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Reporting_Facility = tiledb::Attribute::create<char>(*ctx, "Trade_Reporting_Facility").set_filter_list(attribute_filter_list);
    tiledb::Attribute Participant_Timestamp = tiledb::Attribute::create<uint64_t >(*ctx, "Participant_Timestamp").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Reporting_Facility_TRF_Timestamp = tiledb::Attribute::create<uint64_t>(*ctx, "Trade_Reporting_Facility_TRF_Timestamp").set_filter_list(attribute_filter_list);
    tiledb::Attribute Trade_Through_Exempt_Indicator = tiledb::Attribute::create<uint8_t >(*ctx, "Trade_Through_Exempt_Indicator").set_filter_list(attribute_filter_list);


    schema.add_attributes(Exchange, Symbol, Sale_Condition, Trade_Volume, Trade_Price, Trade_Stop_Stock_Indicator, Trade_Correction_Indicator, Trade_Id,
            Source_of_Trade, Trade_Reporting_Facility, Participant_Timestamp, Trade_Reporting_Facility_TRF_Timestamp, Trade_Through_Exempt_Indicator);

    // Time|Exchange|Symbol|Sale Condition|Trade Volume|Trade Price|Trade Stop Stock Indicator|Trade Correction Indicator|Sequence Number|Trade Id|
    // Source of Trade|Trade Reporting Facility|Participant Timestamp|Trade Reporting Facility TRF Timestamp|Trade Through Exempt Indicator


    // Create the (empty) array on disk.
    tiledb::Array::create(array_uri, schema);
}

int nyse::Trade::load(const std::vector<std::string> file_uris, char delimiter, uint64_t batchSize, uint32_t threads) {
    for (std::string file_uri : file_uris) {
        auto fileSplits = split(file_uri, '_');
        std::unordered_map<std::string, std::string> fileStaticColumns;
        fileStaticColumns.emplace("date", fileSplits.back());
        fileStaticColumns.emplace("datetime", fileSplits.back());
        this->staticColumnsForFiles.emplace(file_uri, fileStaticColumns);
    }
    return Array::load(file_uris, delimiter, batchSize, threads);
}
/*

void nyse::Trade::readSample() {
    array = std::make_unique<tiledb::Array>(*ctx, array_uri, tiledb_query_type_t::TILEDB_READ);
    query = std::make_unique<tiledb::Query>(*ctx, *array);

    query->set_layout(tiledb_layout_t::TILEDB_GLOBAL_ORDER);

    tiledb::ArraySchema arraySchema = array->schema();

    auto coords = std::vector<uint64_t>(this->buffer_size);
    query->set_coordinates(coords);

    auto Exchange = std::vector<char>(this->buffer_size);
    query->set_buffer("Exchange", Exchange);

    auto Symbol = std::vector<std::string>(this->buffer_size);
    auto Symbol_offsets = std::vector<uint64_t>(this->buffer_size);
    query->set_buffer("Symbol", Symbol_offsets, Symbol);

    auto Sale_Condition = std::vector<std::string>(this->buffer_size);
    auto Sale_Condition_offsets = std::vector<uint64_t>(this->buffer_size);
    query->set_buffer("Sale_Condition", Sale_Condition_offsets, Sale_Condition);

    auto Trade_Volume = std::vector<uint32_t>(this->buffer_size);
    query->set_buffer("Trade_Volume", Trade_Volume);

    auto Trade_Price = std::vector<double>(this->buffer_size);
    query->set_buffer("Trade_Price", Trade_Price);

    auto Trade_Stop_Stock_Indicator = std::vector<char>(this->buffer_size);
    query->set_buffer("Trade_Stop_Stock_Indicator", Trade_Stop_Stock_Indicator);

    auto Trade_Correction_Indicator = std::vector<uint8_t>(this->buffer_size);
    query->set_buffer("Trade_Correction_Indicator", Trade_Correction_Indicator);

    auto Trade_Id = std::vector<std::string>(this->buffer_size);
    auto Trade_Id_offsets = std::vector<uint64_t>(this->buffer_size);
    query->set_buffer("Trade_Id", Trade_Id_offsets, Trade_Id);

    auto Source_of_Trade = std::vector<char>(this->buffer_size);
    query->set_buffer("Source_of_Trade", Source_of_Trade);

    auto Trade_Reporting_Facility = std::vector<char>(this->buffer_size);
    query->set_buffer("Trade_Reporting_Facility", Trade_Reporting_Facility);

    auto Participant_Timestamp = std::vector<uint64_t>(this->buffer_size);
    query->set_buffer("Participant_Timestamp", Participant_Timestamp);

    auto Trade_Reporting_Facility_TRF_Timestamp = std::vector<uint64_t>(this->buffer_size);
    query->set_buffer("Trade_Reporting_Facility_TRF_Timestamp", Trade_Reporting_Facility_TRF_Timestamp);

    auto Trade_Through_Exempt_Indicator = std::vector<uint8_t>(this->buffer_size);
    query->set_buffer("Trade_Through_Exempt_Indicator", Trade_Through_Exempt_Indicator);
}*/
