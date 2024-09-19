#!/usr/bin/env bash

make -C ../src &> /dev/null

if [ $? -ne 0 ]; then
    echo "Make failed"
    exit 1
fi

../src/tsc -u $1