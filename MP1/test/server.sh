#!/usr/bin/env bash

make -C ../src &> /dev/null

if [ $? -ne 0 ]; then
    echo "Make failed"
    exit 1
fi

rm -f users.txt *.following *.timeline

../src/tsd