# NYSE Ingestor

This project is a ingestor/loader for loading New York Stock Exchange (NYSE) Quote, Trade and Master data into tiledb.

This project is written in c++.

It requires all delimited files to be local to the file system. Support for s3 csv loading via VFS is disabled as
performance was significantly slower than local data.

## Building

```
mkdir build
cd build
cmake ..
make
```

## Running

Create Master table

```
./NYSE_Ingestor --array "uri" --type Master --create
```

Create Quote table

```
./NYSE_Ingestor --array "uri" --type Quote --create
```

Create Trade table

```
./NYSE_Ingestor --array "uri" --type Trade --create
```

Load Master File

```
./NYSE_Ingestor --array "uri"" -f "./EQY_US_ALL_REF_MASTER_20180306" -d '|'  --type Master 
```

Load Quote File

```
./NYSE_Ingestor --array "uri" -f "./SPLITS_US_ALL_BBO_Z_20180730" -d '|'  --type Quote
```

Load Trade File

```
./NYSE_Ingestor --array "uri"" -f "./EQY_US_ALL_TRADE_20180730" -d '|'  --type Trade
```

## Getting NYSE Sample Data

NYSE sample data is located on their
FTP Server (ftp://ftp.nyxdata.com/Historical%20Data%20Samples/Daily%20TAQ%20Sample%202018/) .

Specification file for delimited formats [Daily_TAQ_Client_Spec_v2.2a.pdf](http://www.nyxdata.com/doc/247075) 