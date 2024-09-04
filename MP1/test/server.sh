#!/usr/bin/env bash

make -C ../src &> /dev/null

rm -f users.txt *.following *.timeline

../src/tsd