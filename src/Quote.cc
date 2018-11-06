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
#include <tiledb/tiledb>

nyse::Quote::Quote(std::string array_name) {
    this->array_uri = std::move(array_name);
    this->ctx = std::make_shared<tiledb::Context>();
}

void nyse::Quote::createArray(tiledb::FilterList coordinate_filter_list, tiledb::FilterList offset_filter_list,
                              tiledb::FilterList attribute_filter_list) {
    // If the array already exists on disk, return immediately.
    if (tiledb::Object::object(*ctx, array_uri).type() == tiledb::Object::Type::Array)
        return;

    tiledb::Domain domain(*ctx);
    // time
/*    domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "datetime", {{0, UINT64_MAX - 1}}, 1000000000));
    domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "datetime", {{0, UINT64_MAX - 1}}, YYYYMMDDHHMMSSXXXXXXXXX));*/


    // Store up to 2 years of data in array
    domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "date", {{1, 20381231}}, 31));

    // Nanoseconds since midnight
    domain.add_dimension(tiledb::Dimension::create<uint64_t>(*ctx, "Time", {{0UL, 235959000000000UL}}, 1000000000UL * 60)); // HHMMSSXXXXXXXXX

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
    tiledb::Attribute symbol = tiledb::Attribute::create<std::string>(*ctx, "Symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute Bid_Price = tiledb::Attribute::create<double>(*ctx, "Bid_Price").set_filter_list(attribute_filter_list);
    tiledb::Attribute Bid_Size = tiledb::Attribute::create<uint32_t>(*ctx, "Bid_Size").set_filter_list(attribute_filter_list);
    tiledb::Attribute Offer_Price = tiledb::Attribute::create<double>(*ctx, "Offer_Price").set_filter_list(attribute_filter_list);
    tiledb::Attribute Offer_Size = tiledb::Attribute::create<uint64_t>(*ctx, "Offer_Size").set_filter_list(attribute_filter_list);
    tiledb::Attribute Quote_Condition = tiledb::Attribute::create<char>(*ctx, "Quote_Condition").set_filter_list(attribute_filter_list);
    tiledb::Attribute National_BBO_Ind = tiledb::Attribute::create<char>(*ctx, "National_BBO_Ind").set_filter_list(attribute_filter_list); // This is listed as numeric but it is alphanumeric (but only a single char)
    tiledb::Attribute FINRA_BBO_Indicator = tiledb::Attribute::create<char>(*ctx, "FINRA_BBO_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute FINRA_ADF_MPID_Indicator = tiledb::Attribute::create<uint8_t>(*ctx, "FINRA_ADF_MPID_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Quote_Cancel_Correction = tiledb::Attribute::create<char>(*ctx, "Quote_Cancel_Correction").set_filter_list(attribute_filter_list);
    tiledb::Attribute Source_Of_Quote = tiledb::Attribute::create<char>(*ctx, "Source_Of_Quote").set_filter_list(attribute_filter_list);
    tiledb::Attribute Retail_Interest_Indicator = tiledb::Attribute::create<char>(*ctx, "Retail_Interest_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Short_Sale_Restriction_Indicator = tiledb::Attribute::create<char>(*ctx, "Short_Sale_Restriction_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute LULD_BBO_Indicator = tiledb::Attribute::create<char>(*ctx, "LULD_BBO_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute SIP_Generated_Message_Identifier = tiledb::Attribute::create<char>(*ctx, "SIP_Generated_Message_Identifier").set_filter_list(attribute_filter_list);
    tiledb::Attribute National_BBO_LULD_Indicator = tiledb::Attribute::create<char>(*ctx, "National_BBO_LULD_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Participant_Timestamp = tiledb::Attribute::create<uint64_t>(*ctx, "Participant_Timestamp").set_filter_list(attribute_filter_list);
    tiledb::Attribute FINRA_ADF_Timestamp = tiledb::Attribute::create<uint64_t>(*ctx, "FINRA_ADF_Timestamp").set_filter_list(attribute_filter_list);
    tiledb::Attribute FINRA_ADF_Market_Participant_Quote_Indicator = tiledb::Attribute::create<char>(*ctx, "FINRA_ADF_Market_Participant_Quote_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Security_Status_Indicator = tiledb::Attribute::create<char>(*ctx, "Security_Status_Indicator").set_filter_list(attribute_filter_list);


    schema.add_attributes(Exchange, symbol, Bid_Price, Bid_Size, Offer_Price, Offer_Size, Quote_Condition, National_BBO_Ind, FINRA_BBO_Indicator, FINRA_ADF_MPID_Indicator,
                          Quote_Cancel_Correction, Source_Of_Quote, Retail_Interest_Indicator, Short_Sale_Restriction_Indicator, LULD_BBO_Indicator, SIP_Generated_Message_Identifier,
                          National_BBO_LULD_Indicator, Participant_Timestamp, FINRA_ADF_Timestamp, FINRA_ADF_Market_Participant_Quote_Indicator, Security_Status_Indicator);

    // Time|Exchange|Symbol|Bid_Price|Bid_Size|Offer_Price|Offer_Size|Quote_Condition|Sequence_Number|National_BBO_Ind|FINRA_BBO_Indicator|FINRA_ADF_MPID_Indicator|
    // National_BBO_LULD_Indicator|Source_Of_Quote|Retail_Interest_Indicator|Short_Sale_Restriction_Indicator|LULD_BBO_Indicator|SIP_Generated_Message_Identifier|
    // National_BBO_LULD_Indicator|Participant_Timestamp|FINRA_ADF_Timestamp|FINRA_ADF_Market_Participant_Quote_Indicator|Security_Status_Indicator


    // Create the (empty) array on disk.
    tiledb::Array::create(array_uri, schema);
}

int nyse::Quote::load(const std::vector<std::string> file_uris, char delimiter, uint64_t batchSize, uint32_t threads) {
    for (std::string file_uri : file_uris) {
        auto fileSplits = split(file_uri, '_');
        std::unordered_map<std::string, std::string> fileStaticColumns;
        fileStaticColumns.emplace("date", fileSplits.back());
        this->staticColumnsForFiles.emplace(file_uri, fileStaticColumns);
    }
    Array::load(file_uris, delimiter, batchSize, threads);
}
