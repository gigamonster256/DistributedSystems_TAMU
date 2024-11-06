#!/usr/bin/env bash

# compile
make -C ../src &> /dev/null

if [ $? -ne 0 ]; then
    echo "Compilation failed"
    exit 1
fi

failed=0
for i in {0..5} ; do
    echo "Test $i"
    ./test${i}.expect &> /dev/null
    if [ $? -ne 0 ]; then
        echo "Test $i failed"
        failed=1
    fi
done

# clean up database
rm -rf server

if [ $failed -eq 1 ]; then
    echo "Some tests failed"
    exit 1
else
    echo "All tests passed"
    exit 0
fi