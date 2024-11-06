#!/usr/bin/env bash

make -C ../src &> /dev/null

if [ $? -ne 0 ]; then
    echo "Make failed"
    exit 1
fi

# clean up database
rm -rf server
mkdir server

../src/tsd 2> server/tsd.err