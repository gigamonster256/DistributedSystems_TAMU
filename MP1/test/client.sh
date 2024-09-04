#!/usr/bin/env bash

make -C ../src &> /dev/null

../src/tsc -u $1