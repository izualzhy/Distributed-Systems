#!/bin/bash

for i in `seq 10`
do
    go test -race -run TestBasic &> out.$i
    if [ $? -ne 0 ]
    then
        echo "$ith test failed."
        break
    fi
    # sleep 5
done
