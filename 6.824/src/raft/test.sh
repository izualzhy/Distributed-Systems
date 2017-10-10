#!/bin/bash

# case_list=(
# TestBasicAgree2B
# TestFailAgree2B
# TestFailNoAgree2B
# TestConcurrentStarts2B
# TestRejoin2B
# TestBackup2B
# TestCount2B)

# for i in ${case_list[@]}
# do
    # for j in `seq 10`
    # do
        # go test -run $i &> out
        # if [ $? -ne 0 ]
        # then
            # echo "run $i failed"
            # exit 1
        # fi
    # done
# done

for i in `seq 10000`
do
    date +%F_%T
    go test -run 2B &> ./res/out.$i
    if [ $? -ne 0 ]
    then
        echo "run failed."
        exit 1
    fi
done
