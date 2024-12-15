#!/usr/bin/env bash

####################################
######## map-reduces tests #########
####################################

#
# compile needed file
#

cargo build --bin mrSequential || exit 1
cargo build --bin mrCoordinator || exit 1
cargo build --bin mrWorker || exit 1

#
# change directory and set exe file path
#

cd mr-input

sequential=../target/debug/mrSequential
coordinator=../target/debug/mrCoordinator
worker=../target/debug/mrWorker

input_files=pg-*.txt

#
# remove tmp file
#

rm -f $(ls | grep -v '^pg-')

####################################
######### word count test ##########
####################################

echo '***' Starting wc test.

# generate correct answer
./$sequential $input_files || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm mr-out-0

./$coordinator 12345 $input_files &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
./$worker wc 12345 &
./$worker wc 12345 &
./$worker wc 12345 &

# wait for the coordinator to exit.
wait $pid

sort *-reduce | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
fi