/**
 * @file  Master.cc
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

#include "Master.h"
#include "buffer.h"
#include <tiledb/tiledb>
#include <CLI11.hpp>
#include <chrono>


nyse::Master::Master(std::string array_name) {
    this->array_uri = std::move(array_name);
    this->ctx = std::make_shared<tiledb::Context>();
}

void nyse::Master::createArray(tiledb::FilterList coordinate_filter_list, tiledb::FilterList offset_filter_list,
                               tiledb::FilterList attribute_filter_list) {

    // If the array already exists on disk, return immediately.
    if (tiledb::Object::object(*ctx, array_uri).type() == tiledb::Object::Type::Array)
        return;

    // The array will be 10000 with dimensions "symbol_id" and extent of 100
    tiledb::Domain domain(*ctx);
    // Setting dimension from 0 to 10k as there are 8614
    domain.add_dimension(tiledb::Dimension::create<int32_t>(*ctx, "symbol_id", {{1, 10000}}, 100));

    // The array will be dense.
    tiledb::ArraySchema schema(*ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
    
    if (coordinate_filter_list.nfilters() > 0) {
        schema.set_coords_filter_list(coordinate_filter_list);
    }
    
    if (offset_filter_list.nfilters() > 0) {
        schema.set_offsets_filter_list(offset_filter_list);
    }

    // Set compression filter to ZSTD if not already set
    if (attribute_filter_list.nfilters() == 0) {
        tiledb::Filter compressor(*ctx, TILEDB_FILTER_ZSTD);
        //f.set_option(TILEDB_COMPRESSION_LEVEL, 5);
        attribute_filter_list.add_filter(compressor);
    }

    // Add a single attribute "a" so each (i,j) cell can store an integer.
    /*tiledb::Attribute symbol = tiledb::Attribute::create<char[16]>(*ctx, "symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute security_description = tiledb::Attribute::create<char[500]>(*ctx, "security_description").set_filter_list(attribute_filter_list);
    tiledb::Attribute cusip = tiledb::Attribute::create<char[9]>(*ctx, "cusip").set_filter_list(attribute_filter_list);
    tiledb::Attribute security_type = tiledb::Attribute::create<char[3]>(*ctx, "security_type").set_filter_list(attribute_filter_list);
    tiledb::Attribute sip_symbol = tiledb::Attribute::create<char[16]>(*ctx, "sip_symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute old_symbol = tiledb::Attribute::create<char[16]>(*ctx, "old_symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute test_symbol_flag = tiledb::Attribute::create<char[1]>(*ctx, "test_symbol_flag").set_filter_list(attribute_filter_list);
    tiledb::Attribute list_exchange = tiledb::Attribute::create<char[1]>(*ctx, "list_exchange").set_filter_list(attribute_filter_list);
    tiledb::Attribute tape = tiledb::Attribute::create<char[1]>(*ctx, "tape").set_filter_list(attribute_filter_list);
    tiledb::Attribute unit_of_trade = tiledb::Attribute::create<int32_t>(*ctx, "unit_of_trade").set_filter_list(attribute_filter_list);
    tiledb::Attribute round_lot = tiledb::Attribute::create<int32_t>(*ctx, "round_lot").set_filter_list(attribute_filter_list);
    tiledb::Attribute nyse_industry_code = tiledb::Attribute::create<char[4]>(*ctx, "nyse_industry_code").set_filter_list(attribute_filter_list);
    tiledb::Attribute shares_outstanding = tiledb::Attribute::create<uint64_t>(*ctx, "shares_outstanding").set_filter_list(attribute_filter_list);
    tiledb::Attribute halt_delay_reason = tiledb::Attribute::create<char[1]>(*ctx, "halt_delay_reason").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_clearing_agent = tiledb::Attribute::create<char[4]>(*ctx, "specialist_clearing_agent").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_clearing_number = tiledb::Attribute::create<int8_t>(*ctx, "specialist_clearing_number").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_post_number = tiledb::Attribute::create<int8_t>(*ctx, "specialist_post_number").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_panel = tiledb::Attribute::create<char[2]>(*ctx, "specialist_panel").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNYSEMKT = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNYSEMKT").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNASDAQBX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNASDAQBX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNSX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNSX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnFINRA = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnFINRA").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnISE = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnISE").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnEdgeA = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnEdgeA").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnEdgeX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnEdgeX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnCHX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnCHX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNYSE = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNYSE").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnArca = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnArca").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNasdaq = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNasdaq").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnCBOE = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnCBOE").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnPSX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnPSX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnBATSY = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnBATSY").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnBATS = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnBATS").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnIEX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnIEX").set_filter_list(attribute_filter_list);
    tiledb::Attribute Tick_Pilot_Indicator = tiledb::Attribute::create<char>(*ctx, "Tick_Pilot_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Effective_Date = tiledb::Attribute::create<std::string>(*ctx, "Effective_Date").set_filter_list(attribute_filter_list);*/

    tiledb::Attribute symbol = tiledb::Attribute::create<std::string>(*ctx, "Symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute security_description = tiledb::Attribute::create<std::string>(*ctx, "Security_Description").set_filter_list(attribute_filter_list);
    tiledb::Attribute cusip = tiledb::Attribute::create<std::string>(*ctx, "CUSIP").set_filter_list(attribute_filter_list);
    tiledb::Attribute security_type = tiledb::Attribute::create<std::string>(*ctx, "Security_Type").set_filter_list(attribute_filter_list);
    tiledb::Attribute sip_symbol = tiledb::Attribute::create<std::string>(*ctx, "SIP_Symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute old_symbol = tiledb::Attribute::create<std::string>(*ctx, "Old_Symbol").set_filter_list(attribute_filter_list);
    tiledb::Attribute test_symbol_flag = tiledb::Attribute::create<char>(*ctx, "Test_Symbol_Flag").set_filter_list(attribute_filter_list);
    tiledb::Attribute listed_exchange = tiledb::Attribute::create<char>(*ctx, "Listed_Exchange").set_filter_list(attribute_filter_list);
    tiledb::Attribute tape = tiledb::Attribute::create<char>(*ctx, "Tape").set_filter_list(attribute_filter_list);
    tiledb::Attribute unit_of_trade = tiledb::Attribute::create<int32_t>(*ctx, "Unit_Of_Trade").set_filter_list(attribute_filter_list);
    tiledb::Attribute round_lot = tiledb::Attribute::create<int32_t>(*ctx, "Round_Lot").set_filter_list(attribute_filter_list);
    tiledb::Attribute nyse_industry_code = tiledb::Attribute::create<std::string>(*ctx, "NYSE_Industry_Code").set_filter_list(attribute_filter_list);
    tiledb::Attribute shares_outstanding = tiledb::Attribute::create<float>(*ctx, "Shares_Outstanding").set_filter_list(attribute_filter_list);
    tiledb::Attribute halt_delay_reason = tiledb::Attribute::create<char>(*ctx, "Halt_Delay_Reason").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_clearing_agent = tiledb::Attribute::create<std::string>(*ctx, "Specialist_Clearing_Agent").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_clearing_number = tiledb::Attribute::create<int8_t>(*ctx, "Specialist_Clearing_Number").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_post_number = tiledb::Attribute::create<int8_t>(*ctx, "Specialist_Post_Number").set_filter_list(attribute_filter_list);
    tiledb::Attribute specialist_panel = tiledb::Attribute::create<std::string>(*ctx, "Specialist_Panel").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNYSEMKT = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNYSEMKT").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNASDAQBX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNASDAQBX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNSX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNSX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnFINRA = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnFINRA").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnISE = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnISE").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnEdgeA = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnEdgeA").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnEdgeX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnEdgeX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnCHX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnCHX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNYSE = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNYSE").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnArca = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnArca").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnNasdaq = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnNasdaq").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnCBOE = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnCBOE").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnPSX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnPSX").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnBATSY = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnBATSY").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnBATS = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnBATS").set_filter_list(attribute_filter_list);
    tiledb::Attribute TradedOnIEX = tiledb::Attribute::create<uint8_t>(*ctx, "TradedOnIEX").set_filter_list(attribute_filter_list);
    tiledb::Attribute Tick_Pilot_Indicator = tiledb::Attribute::create<char>(*ctx, "Tick_Pilot_Indicator").set_filter_list(attribute_filter_list);
    tiledb::Attribute Effective_Date = tiledb::Attribute::create<std::string>(*ctx, "Effective_Date").set_filter_list(attribute_filter_list);
    schema.add_attributes(symbol, security_description, cusip, security_type, sip_symbol, old_symbol, test_symbol_flag, listed_exchange, tape, unit_of_trade, round_lot, nyse_industry_code, shares_outstanding, halt_delay_reason,
                          specialist_clearing_agent, specialist_clearing_number, specialist_post_number, specialist_panel, TradedOnNYSEMKT, TradedOnNASDAQBX, TradedOnNSX, TradedOnFINRA, TradedOnISE,
                          TradedOnEdgeA, TradedOnEdgeX, TradedOnCHX, TradedOnNYSE, TradedOnArca, TradedOnNasdaq, TradedOnCBOE, TradedOnPSX, TradedOnBATSY, TradedOnBATS, TradedOnIEX,
                          Tick_Pilot_Indicator, Effective_Date);


    //Symbol|Security_Description|CUSIP|Security_Type|SIP_Symbol|Old_Symbol|Test_Symbol_Flag|Listed_Exchange|Tape|Unit_Of_Trade|Round_Lot|NYSE_Industry_Code|Shares_Outstanding|Halt_Delay_Reason|
    //Specialist_Clearing_Agent|Specialist_Clearing_Number|Specialist_Post_Number|Specialist_Panel|TradedOnNYSEMKT|TradedOnNASDAQBX|TradedOnNSX|TradedOnFINRA|TradedOnISE|TradedOnEdgeA|TradedOnEdgeX|
    //TradedOnCHX|TradedOnNYSE|TradedOnArca|TradedOnNasdaq|TradedOnCBOE|TradedOnPSX|TradedOnBATSY|TradedOnBATS|TradedOnIEX|Tick_Pilot_Indicator|Effective_Date

    // Create the (empty) array on disk.
    tiledb::Array::create(array_uri, schema);
}

int nyse::Master::load(const std::vector<std::string> file_uris, char delimiter, uint64_t batchSize, uint32_t threads) {
    for (std::string file_uri : file_uris) {
        auto fileSplits = split(file_uri, '_');
        std::unordered_map<std::string, std::string> fileStaticColumns;
        fileStaticColumns.emplace("fake", fileSplits.back());
        this->staticColumnsForFiles.emplace(file_uri, fileStaticColumns);
    }
    Array::load(file_uris, delimiter, batchSize, threads);
}
