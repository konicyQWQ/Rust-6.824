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

#
# set variable
#

failed_any=0

####################################
######### word count test ##########
####################################

echo '***' Starting wc test.

# generate correct answer
./$sequential wc $input_files || exit 1
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

sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

# remove tmp file
rm -f $(ls | grep -v '^pg-')

####################################
########### indexer test ###########
####################################

# generate the correct output
./$sequential indexer $input_files || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

./$coordinator 12345 $input_files &
sleep 1

# start multiple workers
./$worker indexer 12345 &
./$worker indexer 12345

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

rm -f $(ls | grep -v '^pg-')

####################################
########### mtiming test ###########
####################################

echo '***' Starting map parallelism test.

./$coordinator 12345 $input_files &
sleep 1

./$worker mtiming 12345 &
./$worker mtiming 12345

NT=`awk '{print}' mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if awk '{print}' mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

rm -f $(ls | grep -v '^pg-')

####################################
########### rtiming test ###########
####################################

echo '***' Starting reduce parallelism test.

./$coordinator 12345 $input_files &
sleep 1

./$worker rtiming 12345  &
./$worker rtiming 12345

NT=`awk '{print}' mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

rm -f $(ls | grep -v '^pg-')

####################################
########### jobcount test ##########
####################################

echo '***' Starting job count test.

./$coordinator 12345 $input_files &
sleep 1

./$worker jobcount 12345 &
./$worker jobcount 12345
./$worker jobcount 12345 &
./$worker jobcount 12345

NT=`awk '{print}' mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

rm -f $(ls | grep -v '^pg-')

####################################
######### early_exit test ##########
####################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(./$coordinator 12345 $input_files; touch $DF) &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(./$worker early_exit 12345; touch $DF) &
(./$worker early_exit 12345; touch $DF) &
(./$worker early_exit 12345; touch $DF) &

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi

rm -f $(ls | grep -v '^pg-')

##########################################################

echo '***' Starting crash test.

# generate the correct output
./$sequential nocrash $input_files || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
( (./$coordinator 12345 $input_files); touch mr-done ) &
sleep 1

# start multiple workers
./$worker crash 12345 &

SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    ./$worker crash 12345
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    ./$worker crash 12345
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  ./$worker crash 12345
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

rm -f $(ls | grep -v '^pg-')

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi

