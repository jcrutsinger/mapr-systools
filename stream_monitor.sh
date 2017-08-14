#!/bin/bash

STREAM="/streams/stream1"
TOPIC="test"

function sum_of_offset {
  maprcli stream topic info -path $STREAM -topic $TOPIC -json | awk -F':|,' '/maxoffset/ {n+=$2} END {print n}' 2> /dev/null
}

function epoch_ms {
  date +%s%3N
}

date +%T,%3N

o=$(sum_of_offset); t=$(epoch_ms)

while true
do
  o0=$o; t0=$t
  o=$(sum_of_offset); t=$(epoch_ms)
  echo "$(date +%T,%3N) $((($o - $o0)*1000/($t - $t0))) megs/s"
done
 @jcrutsinger
  
            
 
Write