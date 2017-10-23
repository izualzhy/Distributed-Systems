#!/bin/bash

rm res/* -rf
# case_list=(
# TestFigure82C
# )
# TestBasicAgree2B
# TestFailAgree2B
# TestFailNoAgree2B
# TestConcurrentStarts2B
# TestRejoin2B
# TestBackup2B
# TestCount2B)

# for i in ${case_list[@]}
# do
    # for j in `seq 1000`
    # do
        # go test -run $i &> res/$i.$j
        # if [ $? -ne 0 ]
        # then
            # echo "run $i failed"
            # exit 1
        # fi
    # done
# done

# test_list=(
# 2A
# 2B)
# for i in `seq 10000`
# do
    # for j in ${test_list[@]}
    # do
        # date +%F_%T
        # go test -run $j &> ./res/$j.$i
        # if [ $? -ne 0 ]
        # then
            # echo "run failed."
            # exit 1
        # fi
    # done
# done

for i in `seq 10000`
do
    date +%F_%T
    go test &> res/all.$i
    echo "$i result:$?"
    # if [ $? -ne 0 ]
    # then
        # echo "run failed."
        # exit 1
    # fi
done
