#!/bin/bash
#
# By default , DFSIO creates a directory TestDFSIO within /benchmarks in the 
# distributed file system.  This can be overridden via the -Dtest.build.data=/path/to/dir flag. This script allows you to set 
# 
#
#
#
# Test will create <n> files of size <fsize>.  Default to set is 4GB.  this number should be sufficiently large to allow for a long running test, and outstrip local and storage level Caching
#


#########
#
#Variables
#
#
#set this to whatever shows up in yarn-site.xml as your mapreduce.jobhistory.webapp.address , including port # as in the example below

HISTORYSERVER=localhost:19888

# Size of files to be written (in MB, should be 4000, aka 4GB)
fsize=4000

#This is the total filecount , and should equal the TOTAL number of mappers.  For 50 @JPMC Dell 720XD's, this is 50 * 48 = 2400
filecount=2400

#Set the DFS directory where you want to write to.  You will need to use this in the DFSIO read script as well
ROOTDIR=/benchmarks/dfsio


#this is default on hdp2.2 installs, but it may differ based on specific config. 

TJAR=/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient-tests.jar

#############################
#
#Don't change below here
#

#don't change this, needed for collecting results
basename=dfsio_write-$(date -Imin|cut -c-16)
mkdir ${basename}
logname=${basename}/${basename}.log


#Do the write test, and don't clean up when done.

   hadoop jar $TJAR TestDFSIO \
      -Dmapreduce.job.name=DFSIO-write \
      -Dmapreduce.map.cpu.vcores=0 \
      -Dmapreduce.map.memory.mb=768 \
      -Dmapreduce.map.speculative=false \
      -Dmapreduce.reduce.speculative=false \
      -Dtest.build.data=${ROOTDIR} \
      -write -nrFiles $filecount \
      -fileSize $fsize -bufferSize 65536  2>&1 | tee ${logname}


    # Capture the job history log
    myj=$(grep 'INFO mapreduce.Job: Running job' ${logname} |awk '{print $7}')

    curl http://${HISTORYSERVER}/jobhistory/jobcounters/${myj} > ${basename}/${basename}_${myj}.html
    mv TestDFSIO_results.log ${basename}
    cat $0 >> $basename/script.txt
    head -32 $logname  # show the top of the log with elapsed time, etc
