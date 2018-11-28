# Benchmarking

This is a small python program to run benchmarks against NYSE_Ingestor. The config
file contains the list of test suites and tests to run. All configuration
is done via [config.yml](trade.yml).

## Running

Make sure to have compiled NYSE_Ingestor. Change the config to make the base_command
and any other parameters needed

### Pipenv

The program is designed to use pipenv for managing the running environment.

```
pipenv install
```

### Benchmarks

Run via pipenv:
```
pipenv run python3 benchmark.py --config config.yml
```

Example output:

```
+-----------------+------------+--------------------------+---------------------------------+--------------------+---------------------+-----------------------+------------------------------+
|       Test      | Iterations | Ingestion Time (seconds) | Ingestion Time (seconds) STDDEV |  Array Size (MB)   | Ingestion Size (MB) | Export Time (seconds) | Export Time STDDEV (seconds) |
+-----------------+------------+--------------------------+---------------------------------+--------------------+---------------------+-----------------------+------------------------------+
|      bzip2      |     3      |    61.39256556828817     |        1.755533856104893        | 379.3168840408325  |  265.8732604980469  |   0.0650784174601237  |    0.0013154222660428797     |
|       lz4       |     3      |    12.254775524139404    |        2.590544337318459        | 834.0477113723755  |  265.8732604980469  |   74.45254294077556   |      3.295746029133266       |
| byteshuffle_lz4 |     3      |    14.523293018341064    |        7.816472520512526        | 479.95821380615234 |  265.8732604980469  |   109.25852568944295  |      20.257265030558912      |
+-----------------+------------+--------------------------+---------------------------------+--------------------+---------------------+-----------------------+------------------------------+
```
