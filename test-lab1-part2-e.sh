#!/bin/bash

##########################################
#  this file contains:
#   BLOBFILE TEST: test for BLOB big file...
###########################################

DIR=$1
TEST_FILE1=foo.txt
TEST_FILE2=${DIR}/foo.txt
SRCFILE=tmprand
# SRCFILE=test.txt
dd if=/dev/urandom of=${SRCFILE} bs=1K count=400 >/dev/null 2>&1

echo "BLOB FILE TEST"

dd if=${SRCFILE} of=${TEST_FILE1} bs=1K seek=3 count=30 >/dev/null 2>&1
dd if=${SRCFILE} of=${TEST_FILE2} bs=1K seek=3 count=30 >/dev/null 2>&1
# diff ${TEST_FILE1} ${TEST_FILE2} >res.txt 2>&1
if [ $? -ne 0 ];
then
        echo "BLOB FILE TEST FAILED!!!!!!!!!!!!!!!!!!"
        xxd ${TEST_FILE1} > correct.txt
        xxd ${TEST_FILE2} > my.txt
        diff my.txt correct.txt  > diff.txt 2>&1
        exit
fi
echo "Passed BLOB test"

rm ${TEST_FILE2} ${TEST_FILE1} ${SRCFILE}
