#!/bin/bash
#
#This script will run terasort against previously generated data from Teragen.  Number of mappers is determined by previous Teragen run
# Number of reducers should be set to 8 per compute node.
#


#########
#
#Variables
#
#
#set this to whatever shows up in yarn-site.xml as your mapreduce.jobhistory.webapp.address , including port # as in the example below

HISTORYSERVER=localhost:19888




#Set this to where you had teragen write to
INPUTDIR=/benchmarks/tera/in
#Set this to your output dir.  It will be created if it does not exist
OUTPUTDIR=/benchmarks/tera/run1

#number of reducers per compute node.  On JPMC Dell 720XD's, this number should be 8.
rtasks=8

#defaults for Mapper and Reducer memory per task/container.  Mappers should get 1024, and Reducers 3072
MAPMEM=1024
REDMEM=3072


#default location on HDP2.2, this may differ slightly depending on distro version and installation methods.
TJAR=/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar


##########################
#
#Don't change below here
#
basename=terasort-run1-$(date -Imin|cut -c-16)
mkdir ${basename}
logname=${basename}/${basename}.log

    hadoop fs -rm -r ${OUTPUTDIR}
    hadoop jar $TJAR terasort \
    -Dmapreduce.reduce.memory.mb=$REDMEM \
    -Dmapreduce.map.memory.mb=$MAPMEM \
    -Dmapred.maxthreads.generate.mapoutput=2 \
    -Dmapreduce.tasktracker.reserved.physicalmemory.mb.low=0.95 \
    -Dmapred.maxthreads.partition.closer=2 \
    -Dmapreduce.map.sort.spill.percent=0.99 \
    -Dmapreduce.reduce.merge.inmem.threshold=0 \
    -Dmapreduce.job.reduce.slowstart.completedmaps=1 \
    -Dmapreduce.reduce.shuffle.parallelcopies=40 \
    -Dmapreduce.map.speculative=false \
    -Dmapreduce.reduce.speculative=false \
    -Dmapreduce.map.output.compress=false \
    -Dmapreduce.task.io.sort.mb=480 \
    -Dmapreduce.task.io.sort.factor=400 \
    -Dmapreduce.job.reduces=$rtasks \
    ${INPUTDIR} ${OUTPUTDIR} 2>&1 | tee ${logname} 

    sleep 1

    # Capture the job history log
    myj=$(grep 'INFO mapreduce.Job: Running job' ${logname} |awk '{print $7}')

    curl http://${HISTORYSERVER}/jobhistory/jobcounters/${myj} > ${basename}/${basename}_${myj}.html
    #mapred job -history $myf > $logname  # capture the run log
    cat $0 >> ${basename}/script.txt 
    head -32 $logname  # show the top of the log with elapsed time, etc

