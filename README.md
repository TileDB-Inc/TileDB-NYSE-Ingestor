# NYSE Ingestor

This project is a ingestor/loader for loading New York Stock Exchange (NYSE) Quote, Trade and Master data into tiledb.

This project is written in c++.

It requires all delimited files to be local to the file system. 

The Master file is used to to create an `symbol_id` numerical value to map the
stock symbol to the master file. This is equivalent of an auto increment id
in a traditional RDBMS.

## Building

The NYSE Ingestor is configured for a superbuild. If TileDB or the date library
is not found they will be downloaded and compiled from source.

```
mkdir build
cd build
cmake ..
make
```

## Running

Create Master table

```
./NYSE_Ingestor/NYSE_Ingestor --array "uri" --type Master --create
```

Create Quote table

```
./NYSE_Ingestor/NYSE_Ingestor --array "uri" --type Quote --create
```

Create Trade table

```
./NYSE_Ingestor/NYSE_Ingestor --array "uri" --type Trade --create
```

Load Master File

```
./NYSE_Ingestor/NYSE_Ingestor --array "uri"" -f "./EQY_US_ALL_REF_MASTER_20180306" -d '|'  --type Master 
```

Load Quote File

```
./NYSE_Ingestor/NYSE_Ingestor --array "uri" -f "./SPLITS_US_ALL_BBO_Z_20180730" -d '|'  --type Quote --master_file "./EQY_US_ALL_REF_MASTER_20180306"
```

Load Trade File

```
./NYSE_Ingestor/NYSE_Ingestor --array "uri"" -f "./EQY_US_ALL_TRADE_20180730" -d '|'  --type Trade --master_file "./EQY_US_ALL_REF_MASTER_20180306"
```

## Getting NYSE Sample Data

NYSE sample data is located on their
FTP Server (ftp://ftp.nyxdata.com/Historical%20Data%20Samples/Daily%20TAQ%20Sample%202018/) .

Specification file for delimited formats [Daily_TAQ_Client_Spec_v2.2a.pdf](http://www.nyxdata.com/doc/247075) 
