# TileDB NYSE Data Ingestor

This project is an ingestor/loader written in C++ for loading New York Stock Exchange (NYSE) Quote, Trade and Master data into [TileDB](https://github.com/TileDB-Inc/TileDB).

## Getting the NYSE Sample Data

The NYSE sample data is located on the `nyxdata` FTP Server:

`ftp://ftp.nyxdata.com/Historical%20Data%20Samples/Daily%20TAQ%20Sample%202018/`

Here is the specification file for delimited formats: [Daily_TAQ_Client_Spec_v2.2a.pdf](http://www.nyxdata.com/doc/247075) 

### Master Files

File name format: `EQY_US_ALL_REF_MASTER_<date>`

Master files contain dimensional data, i.e., metedata about every stock that was
listed on the NYSE exchange for a given day.

The master file will be loaded into a TileDB array. It is also required for
loading [Quote Files](#quote-files) or [Trade Files](#trade-files). The master file is
used to to create a `symbol_id` numerical value mapped to each stock symbol. 
This is equivalent to an auto increment id in a traditional RDBMS.

### Quote Files

The quote files are split into 26 alphabetic files based on the stock symbol.

File name format: `SPLITS_US_ALL_BBO_<letter>_<date>`

Each quote identifies the time, exchange, security, bid/ask volumes, bid/ask
prices, NBBO indicator, and more. See the 
[specification file](http://www.nyxdata.com/doc/247075) for the full list.

### Trade Files

File name format: `EQY_US_ALL_TRADE_<date>`

Each trade identifies the time, exchange, security, volume, price, sale
condition, and more. See the 
[specification file](http://www.nyxdata.com/doc/247075) for the full list.

## Building

The NYSE Ingestor is configured for a superbuild. If TileDB or the date library
is not found, they will be automatically downloaded and compiled from source.

```
mkdir build
cd build
cmake ..
make -j$(nproc)
```

## Usage

### Load a master file

First create a master TileDB array to store the master data.

```
./nyse_ingestor/nyse_ingestor --array "master_array" --type Master --create

```

Then load a master file into the array as follows.

```
./nyse_ingestor/nyse_ingestor --array "master_array" -f "../sample_data/small_EQY_US_ALL_REF_MASTER_20180306" --type Master
```

### Load a quote file 

First create a quote TileDB array to store the quote data.

```
./nyse_ingestor/nyse_ingestor --array "quote_array" --type Quote --create
```

Then load a quote file into the array as follows.

```
./nyse_ingestor/nyse_ingestor --array "quote_array" -f "../sample_data/small_SPLITS_US_ALL_BBO_Z_20180730" --type Quote --master_file "../sample_data/small_EQY_US_ALL_REF_MASTER_20180306"
```

### Load a trade file

First create a trade TileDB array to store the trade data.

```
./nyse_ingestor/nyse_ingestor --array "trade_array" --type Trade --create
```

Then load a trade file into the array as follows.

```
./nyse_ingestor/nyse_ingestor --array "trade_array" -f "../sample_data/small_EQY_US_ALL_TRADE_20180730" --type Trade --master_file "../sample_data/small_EQY_US_ALL_REF_MASTER_20180306"
```

## Setting TileDB Filters

[Filters](https://docs.tiledb.io/en/stable/tutorials/filters.html) are applied
to Attributes, Coordinates and Offsets to provide compression and other
options. By default the TileDB arrays will be created using the following
filters:

| Component | Filter List |
| --------- | ----------- |
| Attributes | ZSTD Compressor |
| Coordinates | Double Delta, ZSTD Compressor |
| Offsets | Double Delta, ZSTD Compressor |

Each one of these filter pipelines can be adjusted by passing a CSV list
of filter names via the relevant command line flag during the create array
command.

```
  --coordinate_filters TEXT ... List of filters to apply to coordinates
  --offset_filters TEXT ...     List of filters to apply to offsets
  --attribute_filters TEXT ...  List of filters to apply to attributes
```

The available filters that can be passed are:

| Filter | Recognized Command Line String |
| ------ | ------------------------------ |
| TILEDB_FILTER_NONE | NOOP |
| TILEDB_FILTER_GZIP | GZIP |
| TILEDB_FILTER_ZSTD | ZSTD |
| TILEDB_FILTER_LZ4 | LZ4 |
| TILEDB_FILTER_RLE | RLE |
| TILEDB_FILTER_BZIP2 | BZIP2 |
| TILEDB_FILTER_DOUBLE_DELTA | DOUBLE_DELTA |
| TILEDB_FILTER_BIT_WIDTH_REDUCTION | BIT_WIDTH_REDUCTION |
| TILEDB_FILTER_BITSHUFFLE | BITSHUFFLE |
| TILEDB_FILTER_BYTESHUFFLE | BYTESHUFFLE |
| TILEDB_FILTER_POSITIVE_DELTA | POSITIVE_DELTA |

### Usage

As an example, to use a gzip filter for attributes, and double delta + gzip for coordinates and
offsets, use the following arguments:

```
./nyse_ingestor/nyse_ingestor --array "quote_array_gzip" --type Quote --create --coordinate_filters DOUBLE_DELTA,GZIP --offset_filters DOUBLE_DELTA,GZIP --attribute_filters GZIP
```

