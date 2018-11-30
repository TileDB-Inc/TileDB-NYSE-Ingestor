# NYSE Ingestor

This project is a ingestor/loader for loading New York Stock Exchange (NYSE) Quote, Trade and Master data into [TileDB](https://github.com/TileDB-Inc/TileDB).

This project is written in C++.

It requires all delimited files to be on a local filesystem. 

The master file is used to to create an `symbol_id` numerical value to map the
stock symbol to the master file. This is equivalent of an auto increment id
in a traditional RDBMS.


## Getting NYSE Sample Data

NYSE sample data is located on the nyxdata FTP Server:

`ftp://ftp.nyxdata.com/Historical%20Data%20Samples/Daily%20TAQ%20Sample%202018/`

Specification file for delimited formats: [Daily_TAQ_Client_Spec_v2.2a.pdf](http://www.nyxdata.com/doc/247075) 

## Building

The NYSE Ingestor is configured for a superbuild. If TileDB or the date library
is not found they will be automatically downloaded and compiled from source.

```
mkdir build
cd build
cmake ..
make -j$(nproc)
```

## Running Ingestor

Below are example commands for creating the arrays and loading sample data included in this repository.
The sample data included is just the first 100 lines of each file.


### Create Master table

```
./nyse_ingestor/nyse_ingestor --array "master_array" --type Master --create
```

### Create Quote table

```
./nyse_ingestor/nyse_ingestor --array "quote_array" --type Quote --create
```

### Create Trade table

```
./nyse_ingestor/nyse_ingestor --array "trade_array" --type Trade --create
```

### Load Sample Master File

```
./nyse_ingestor/nyse_ingestor --array "master_array" -f "../sample_data/small_EQY_US_ALL_REF_MASTER_20180306" --type Master
```

### Load Sample Quote File

```
./nyse_ingestor/nyse_ingestor --array "quote_array" -f "../sample_data/small_SPLITS_US_ALL_BBO_Z_20180730" --type Quote --master_file "../sample_data/small_EQY_US_ALL_REF_MASTER_20180306"
```

### Load Sample Trade File

```
./nyse_ingestor/nyse_ingestor --array "trade_array" -f "../sample_data/small_EQY_US_ALL_TRADE_20180730" --type Trade --master_file "../sample_data/small_EQY_US_ALL_REF_MASTER_20180306"
```

## Altering TileDB Array Filters

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

### Example Filter Usage

To use a gzip filter for attributes, and double delta + gzip for coordinates and
offsets, use the following arguments:

```
./nyse_ingestor/nyse_ingestor --array "quote_array_gzip" --type Quote --create --coordinate_filters DOUBLE_DELTA,GZIP --offset_filters DOUBLE_DELTA,GZIP --attribute_filters GZIP
```

