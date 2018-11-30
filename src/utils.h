/**
 * @file  utils.h
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
 * Utils for ingestor
 *
 */

#ifndef NYSE_INGESTOR_UTILS_H
#define NYSE_INGESTOR_UTILS_H

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <tiledb/tiledb>

namespace nyse {
/**
 * Helper function for nice duration printing
 * @param input_seconds
 * @return
 */
static std::string beautify_duration(std::chrono::seconds input_seconds) {
  using namespace std::chrono;
  typedef duration<int, std::ratio<86400>> days;
  auto d = duration_cast<days>(input_seconds);
  input_seconds -= d;
  auto h = duration_cast<hours>(input_seconds);
  input_seconds -= h;
  auto m = duration_cast<minutes>(input_seconds);
  input_seconds -= m;
  auto s = duration_cast<seconds>(input_seconds);

  auto dc = d.count();
  auto hc = h.count();
  auto mc = m.count();
  auto sc = s.count();

  std::stringstream ss;
  ss.fill('0');
  if (dc) {
    ss << d.count() << "d";
  }
  if (dc || hc) {
    if (dc) {
      ss << std::setw(2);
    } // pad if second set of numbers
    ss << h.count() << "h";
  }
  if (dc || hc || mc) {
    if (dc || hc) {
      ss << std::setw(2);
    }
    ss << m.count() << "m";
  }
  if (dc || hc || mc || sc) {
    if (dc || hc || mc) {
      ss << std::setw(2);
    }
    ss << s.count() << 's';
  }

  return ss.str();
}

/**
 * Create a filter list from a csv string
 * @param ctx
 * @param filter_list
 * @param filters
 */
static void
create_filter_list_from_str(tiledb::Context ctx,
                            tiledb::FilterList &filter_list,
                            const std::vector<std::string> &filters) {
  for (auto &filter_str : filters) {
    if (filter_str == "NOOP") {
      filter_list.add_filter({ctx, TILEDB_FILTER_NONE});
    } else if (filter_str == "GZIP") {
      filter_list.add_filter({ctx, TILEDB_FILTER_GZIP});
    } else if (filter_str == "ZSTD") {
      filter_list.add_filter({ctx, TILEDB_FILTER_ZSTD});
    } else if (filter_str == "LZ4") {
      filter_list.add_filter({ctx, TILEDB_FILTER_LZ4});
    } else if (filter_str == "RLE") {
      filter_list.add_filter({ctx, TILEDB_FILTER_RLE});
    } else if (filter_str == "BZIP2") {
      filter_list.add_filter({ctx, TILEDB_FILTER_BZIP2});
    } else if (filter_str == "DOUBLE_DELTA") {
      filter_list.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA});
    } else if (filter_str == "BIT_WIDTH_REDUCTION") {
      filter_list.add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION});
    } else if (filter_str == "BITSHUFFLE") {
      filter_list.add_filter({ctx, TILEDB_FILTER_BITSHUFFLE});
    } else if (filter_str == "BYTESHUFFLE") {
      filter_list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE});
    } else if (filter_str == "POSITIVE_DELTA") {
      filter_list.add_filter({ctx, TILEDB_FILTER_POSITIVE_DELTA});
    }
  }
}
} // namespace nyse

#endif // NYSE_INGESTOR_UTILS_H
