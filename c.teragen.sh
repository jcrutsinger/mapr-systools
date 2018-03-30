#!/bin/bash
#
#This script generates data for Teragen.  The number of MAPS is important, as it will pass through to the subsequent terasort run
#
#########
#
#Variables
#
#
# set this to whatever shows up in yarn-site.xml as your mapreduce.jobhistory.webapp.address , including port # as in the example below

HISTORYSERVER=localhost:19888

#set this to where you want to write to, terasort will need to be pointed here as well 
INPUTDIR=/benchmarks/tera/in

# The number of maps should be set to 24 per compute-node.  For a 50-node test, this is 1200
MAPS=1200

# TeraGen (specify size using 100 Byte records, 1TB = $[10*1000*1000*1000])
SIZE=$[10*1000*1000*1000]

####################
#
#Don't change below here
#

basename=teragen-$(date -Imin|cut -c-16)

mkdir ${basename}
logname=${basename}/${basename}.log



    hadoop fs -rm -r ${INPUTDIR}

TJAR=/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar

    hadoop jar $TJAR teragen \
   -Dmapreduce.job.maps=$MAPS \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   -Dmapreduce.map.disk=0 \
   $SIZE \
   ${INPUTDIR} 2>&1 | tee ${logname} 

sleep 1


    # Capture the job history log
    myj=$(grep 'INFO mapreduce.Job: Running job' ${logname} |awk '{print $7}')

    curl http://${HISTORYSERVER}/jobhistory/jobcounters/${myj} > ${basename}/${basename}_${myj}.html
    cat $0 >> $basename/script.txt
    head -32 $logname  # show the top of the log with elapsed time, etc


