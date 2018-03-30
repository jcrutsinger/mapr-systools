#!/bin/bash
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
echo $SCRIPT
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")
echo $SCRIPTPATH
TEST_HOME=$SCRIPTPATH/..
echo $TEST_HOME
. $TEST_HOME/profile
#pig -x mapreduce -P ./pig.properties -p date='2014-01-28' -p rawlog='/user/test/rawlogs/' -p dayLogLocation='2014-01-28' -p nextDayLogLocation='2014-01-29' -p PIGDIR='/usr/local/pig/pig-0.11.1' ./r1.pig
pig -x mapreduce -P $SCRIPTPATH/pig.properties -p scriptpath="$SCRIPTPATH" -p date='2014-02-02' -p rawlog='/user/test/rawlogs/' -p dayLogLocation='2014-02-02' -p nextDayLogLocation='2014-02-03' -p PIGDIR="$PIGDIR" $SCRIPTPATH/r1.pig

