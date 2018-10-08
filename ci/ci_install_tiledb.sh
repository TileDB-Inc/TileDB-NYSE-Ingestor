#!/bin/bash

set -v -x

original_dir=$PWD

# Install tiledb using git dev branch until 1.3 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/TileDB-Inc/TileDB.git && cd TileDB \
&& mkdir -p build && cd build

# Configure and build TileDB
cmake -DTILEDB_VERBOSE=ON -DTILEDB_STATIC=ON .. \
&& make -j4 \
&& sudo make -C tiledb install

cd $original_dir

set +v +x
