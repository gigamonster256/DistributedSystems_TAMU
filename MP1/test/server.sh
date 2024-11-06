#!/usr/bin/env bash

# clean up database
rm -rf server
mkdir server

../src/tsd 2> server/tsd.err