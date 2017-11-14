#!/bin/bash

#set -x
function usage(){
	echo 
	echo "Usage : "
    echo "./$me <TIMESTAMP|DIR> [Options]"

    echo " Options : "
    echo -e "\t -p | --parse | parse" 
    echo -e "\t\t - Parse the YCSB Logs & print metrics"
    echo -e "\t -a | --analyze | analyze" 
    echo -e "\t\t - Analyse the YCSB logs for ERROR/EXCEPTION(s)"
    echo -e "\t -s=<SEARCHKEYWORD> | --search=<SEARCHKEYWORD> | search=<SEARCHKEYWORD>" 
    echo -e "\t\t - Search the YCSB logs for SEARCHKEYWORD"
    echo -e "\t -d=<RUN_DESCRIPTION> | --desc=<RUN_DESCRIPTION> | desc=<RUN_DESCRIPTION>" 
    echo -e "\t\t - Description of the test run (to be used in puffd)"
    echo -e "\t -y" 
    echo -e "\t\t - Continue without prompting"
    echo -e "\t -g" 
    echo -e "\t\t - Publish metrics to graphite"
    echo -e "\t -post" 
    echo -e "\t\t - Post to Database(puffd)"
   	echo -e "\t -plot" 
    echo -e "\t\t - Post to Run Latency trends to Puffd"
}


logsdir=$PWD
libdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
me=$(basename $BASH_SOURCE)
#echo "LIB DIR : $libdir"
if [[ "$libdir" == */logs ]]; then
	logsdir=$libdir
elif [ -d "$libdir/logs" ]; then
	logsdir=$libdir/logs
elif [ -d "$libdir/../logs" ]; then
	logsdir="$libdir/../logs"
fi
#echo "Logs DIR : $logsdir"

caltimestamp=
bzfiles=$(ls -f $logsdir/ycsb*.bz2 2>/dev/null)
if [ -n "$bzfiles" ]; then
	caltimestamp=$(ls -t $logsdir/ycsb*.bz2 2>/dev/null | awk -F/ '{print $NF}'| head -n 1 | cut -d'_' -f2)
else
	caltimestamp=$(ls -td $logsdir/*/ 2>/dev/null | head -n 1 | tr '/' ' ' | awk -F' ' '{print $NF}')
fi
#echo "Calculated timestamp : $caltimestamp"

header="Threads Throughput AvgLat(μs) MinLat(μs) MaxLat(μs) 95%ile(ms) 99%ile(ms) RunTime(s)"
runheader="DataSize(GB) RowSize RowCount Regions ClientNodes"
clusterheader="CPUs Memory(GB) DiskSpace(TB) Network OS DataNodes Build #clients/node"
idxlagheader="WrkStart WrkEnd Time(s) IndexStart IndexEnd Time(s) AvgLat(s) MaxLat(s)"
runcolw=20
ccolw=17
idxcolw=12
headeron=0
firstcollen=0
colw=15
cscolw=20
wrkldthread=
postcnt=0
TIMESTAMP=
GRAPHITEHOST=10.10.88.136
GRAPHITEPORT=2003
twfile="tablewatch_out.log"

if [ -e "$1" ] && [ -d "$1" ]; then
	TIMESTAMP=$1
	if [[ "$TIMESTAMP" == *\/* ]]; then
		onlyts=$(echo $TIMESTAMP | sed 's/.*\///')
		logsdir=${TIMESTAMP%%$onlyts}
	else
		logsdir="."
	fi
	TIMESTAMP=$(echo $TIMESTAMP | sed 's/.*\///')
fi

re='^[0-9]+$'
if [[ $1 =~ $re ]]; then
	TIMESTAMP=$1
fi

#  @param latency
#  @param workload id
#  @param keyword
function getStat(){
	local retstat=$(for i in `find -type d -name '*logs*'`;do grep $1 $i/$2_status.log 2>/dev/null| grep "$3";done | awk '{sum+=$3; count++} END { if ($count != 0) print sum/count}')
	echo "$retstat"
}

#  @param workload id
function getRunTime(){
	local startts=$(getStartTime "$1")
	local endts=$(getEndTime "$1")
	local tsdiff=$(echo "$endts-$startts" | bc)
	echo "$tsdiff"
}

function getStartTime(){
	local dirstr=$2
	[ -z "$dirstr" ] && dirstr="."
	local timestamps=$(for i in $(find $dirstr -name $1_out.log); do grep " 0 sec:" $i | awk '{print $1,$2}' ; done)
	local startts=
	while read -r ts; do
		ts=${ts%:*}
		local logts=$(date -d "$ts" +%s)
		[ -z "$startts" ] && startts=$logts
		[ "$logts" -lt "$startts" ] && startts=$logts
	done <<< "$timestamps"
	echo "$startts"
}

function getEndTime(){
	local dirstr=$2
	[ -z "$dirstr" ] && dirstr="."
	local timestamps=$(for i in $(find $dirstr -name $1_out.log); do tail -n 100 $i | grep -v " 0 current" | tail -1 | awk '{print $1,$2}' ; done)
	local endts=
	while read -r ts; do
		ts=${ts%:*}
		local logts=$(date -d "$ts" +%s)
		[ -z "$endts" ] && endts=$logts
		[ "$logts" -gt "$endts" ] && endts=$logts
	done <<< "$timestamps"
	echo "$endts"
}

#  @param workload id
#  @param keyword
function grepWorkloadLog(){
	local retstat=$(for i in `find -type d -name *logs* ! -name *status*`;do grep $2 $i/$1_*.log 2>/dev/null| grep -v status.log;done)
	local cnt=$(echo "$retstat" | wc -l)
	if [ -n "$retstat" ]; then 
		echo -e "\t  Found $cnt '$2' in run logs"
		echo -e "$retstat" | sed 's/^/\t\t/' | tail -n 10
	fi
}

#  @param workload id
#  @param keyword
function grepWorkloadStatusLog(){
	local retstat=$(for i in `find -type d -name *logs*`;do grep -i "Return=ERROR" $i/$1_*.log 2>/dev/null| grep -v out.log;done)
	local cnt=$(echo "$retstat" | wc -l)
	if [ -n "$retstat" ]; then 
		echo -e "\t  Found $cnt clients with failed ops in status logs"
		echo -e "$retstat" | sed 's/^/\t\t/'
	fi
}

#  @param workload id
#  @param keyword
function grepWorkloadGCLog(){
	if [ -z "$1" ] || [ -z "$2" ]; then
		return
	fi
	for node in $(echo */)
	do
		cd $node

		local retstat=$(for i in $(find -type f -name $1_gc.log);do grep -i "$2" $i;done)
		local cnt=$(echo "$retstat"	| wc -l)
		if [ -n "$cnt" ] && [ "$cnt" -gt 1 ]; then
			echo -e "\tFound '$2' $cnt times in directory $node"
		fi
		cd ..
	done
	
}

#  @param keyword
function grepMFSLogs(){
	if [ -z "$1" ]; then
		return
	fi
	local runcmd="for i in \$(find -type f -name mfs.log*); do "
	local i=0
	for key in "$@"
	do
		if [ "$i" -gt 0 ]; then
			runcmd=$runcmd" | grep '$key'"
		else
			runcmd=$runcmd" grep '$key' \$i"
		fi
		let i=i+1
	done
	runcmd=$runcmd"; done"

	for node in $(echo */)
	do
		cd $node
		local retstat=$(bash -c "$runcmd")
		local cnt=$(echo "$retstat"	| wc -l)
		if [ -n "$retstat" ] && [ -n "$cnt" ]; then
			echo -e "\tSearchkey found $cnt times in directory $node"
			echo -e "\t\t$retstat" | head -n 1
		fi
		cd ..
	done
}

#  @param keyword
function grepAllLogs(){
	local retstat=$(for i in `find -type d -name "*logs*"`;do grep $1 $i/*.log;done)
	echo "$retstat"
}

#  @param latency
#  @param workload id
#  @param keyword
function getMinStat(){
	local retstat=$(for i in `find -type d -name '*logs*'`;do grep $1 $i/$2_status.log 2>/dev/null| grep "$3";done |  awk '{print $3}'| sort -n | head -1)
	echo "$retstat"
}

#  @param latency
#  @param workload id
#  @param keyword
function getMaxStat(){
	local retstat=$(for i in `find -type d -name '*logs*'`;do grep $1 $i/$2_status.log 2>/dev/null| grep "$3";done |  awk '{print $3}'| sort -n | tail -1)
	echo "$retstat"
}

#  @param workload id
function getThroughput(){
	local tp=$(getStat "Throughput" "$1" "OVERALL")
	echo "$tp"
}

#  @param workload id
#  @param keyword
function getAvgLatency(){
	local avglat=$(getStat "AverageLatency" "$1" "$2")
	echo "$avglat"
}

#  @param workload id
#  @param keyword
function getMinLatency(){
	local minlat=$(getMinStat "MinLatency" "$1" "$2")
	echo "$minlat"
}

#  @param workload id
#  @param keyword
function getMaxLatency(){
	local maxlat=$(getMaxStat "MaxLatency" "$1" "$2")
	echo "$maxlat"
}

#  @param workload id
#  @param keyword
function get95thPercentileLatency(){
	local var95thlat=$(getStat "95thPercentileLatency" "$1" "$2")
	echo "$var95thlat"
}

#  @param workload type
#  @param workload id
#  @param keyword
function get99thPercentileLatency(){
	local var99thlat=$(getStat "99thPercentileLatency" "$1" "$2")
	echo "$var99thlat"
}

declare -A resultarr
nodecnt=0
cpus=0
mem=0
diskspace=0
nwspeed=0
os=UNKNOWN
build=UNKNOWN
driver=UNKNOWN
cpn=
rowsize=

function printClusterInfo(){
	if [ ! -e "/usr/bin/maprcli" ]; then
		return
	fi
	local headers=($clusterheader)
	local dashfile="/tmp/dashboard.txt"
	maprcli dashboard info -json > $dashfile

	if [ $(stat --printf="%s" $dashfile) -lt 256 ]; then
		return
	fi

	nodecnt=$(maprcli node list -json | grep -A2 hostname | sed '/hostname/{$!N;/\n.*cldb/!P;D}' | grep hostname | tr -d "\t" | tr -d "\"" | tr -d "," | sed 's/hostname://g' | wc -l)
	#local nodedisklist="$(maprcli disk listall | grep MapR-FS | awk '{$2=$2};1' | cut -d' ' -f1,5,12)"
	local utilline=$(grep -nr 'utilization' $dashfile | cut -d':' -f1)
	cpus=$(tail -n +$utilline $dashfile | head -n 6 | grep total | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
	local cpustring=$(echo "$cpus/$nodecnt"|bc)
	let utilline=utilline+6
	mem=$(tail -n +$utilline $dashfile | head -n 4 | grep total | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
	local localmem=$(echo "$(grep MemTotal /proc/meminfo | awk '{print $2}')/1048576" | bc)
	#local memstr=$(echo "$mem/$nodecnt"|bc)
	let utilline=utilline+4
	diskspace=$(tail -n +$utilline $dashfile | head -n 4 | grep total | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
	local netinf=$(ip -o link show | awk '{print $2,$9}' | grep UP | head -1| cut -d':' -f1)
	nwspeed=$(ethtool $netinf | grep Speed | tr -d "\t" | tr -d " " | cut -d":" -f2)
	local osname=$(lsb_release -a | grep Distributor | tr -d '\t' | cut -d':' -f2)
	local osver=$(lsb_release -a | grep Release | tr -d '\t' | cut -d':' -f2)
	os="$osname $osver"
	build=$(cat /opt/mapr/MapRBuildVersion)
	[ -d "/opt/mapr/.patch/" ] && build=$build"(patch "$(ls /opt/mapr/.patch/README.* | cut -d '.' -f3 | tr -d 'p')")"
	cpn=$(echo "$nodecnt/$clientcnt" | bc)
	driver=$(ip addr | grep 'state UP' -A2 | head -n 3 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')

	for col in ${headers[@]}
	do
		if [[ "$col" = "CPUs" ]] || [[ "$col" == Memory* ]]; then 
			printf "\e[33m\e[4m%-${ccolw}s\e[0m\e[0m" "$col"
		else
			printf "\e[33m\e[4m%${ccolw}s\e[0m\e[0m" "$col"
		fi
	done
	echo

	printf "\e[36m%-${ccolw}s\e[0m" "$cpus ($cpustring x $nodecnt)"
	printf "\e[36m%-${ccolw}s\e[0m" "$(echo "$mem/1024"|bc) ($localmem x $nodecnt)"
	printf "\e[36m%${ccolw}s\e[0m" "$(echo "$diskspace/1024"|bc)"
	printf "\e[36m%${ccolw}s\e[0m" "$nwspeed"
	printf "\e[36m%${ccolw}s\e[0m" "$os"
	printf "\e[36m%${ccolw}s\e[0m" "$nodecnt"
	printf "\e[36m%${ccolw}s\e[0m" "$build"
	printf "\e[36m%${ccolw}s\e[0m" "$cpn"

	echo
	echo
}

function printRunInfo(){
	local rundate=$(date -d @$(echo "($TIMESTAMP + 500)/1000"|bc) +'%Y-%m-%d %H:%M:%S')
	local dsize=$(echo "$datasize/1073741824"|bc)
	local headers=($runheader)
	local rowsizeK=$(echo "($fieldscount*$fieldslength)/1000"|bc)
	if [ "$multicf" = "true" ]; then
		rowsizeK="10"
		local cf2len=$(echo "$fieldslength*90"|bc)
		rowsize="(${fieldscount}x${fieldslength}, 1x$cf2len)"
	else
		rowsize="(${fieldscount}x${fieldslength})"
	fi
	printf "\e[33m\e[4m%-22s\e[0m\e[0m" "RunTime"
	 
	for col in ${headers[@]}
	do
		printf "\e[33m\e[4m%${runcolw}s\e[0m\e[0m" "$col"
	done
	echo
	printf "\e[36m%-22s\e[0m" "$rundate"
	printf "\e[36m%${runcolw}s\e[0m" "$dsize"
	printf "\e[36m%${runcolw}s\e[0m" "${rowsizeK}K $rowsize"
	printf "\e[36m%${runcolw}s\e[0m" "$numrows"
	printf "\e[36m%${runcolw}s\e[0m" "$numregions"
	printf "\e[36m%${runcolw}s\e[0m" "$clientcnt"
	echo
	echo
}

function printHeader(){
	local headers=($header)
	[ -n "$1" ] && headers=($1)
	if [ "$headeron" -eq 0 ]; then
		let firstcollen=firstcollen+1
		printf "\e[33m\e[4m%-${firstcollen}s\e[0m\e[0m" "Workload"
		for col in ${headers[@]}
		do
			local collen=$colw
			# Special character 'μ' causes padding to shift by one
			[ -n "$(echo $col | grep Lat)" ] && let collen=collen+1
			printf "\e[33m\e[4m%${collen}s\e[0m\e[0m" "$col"
		done
		echo
		headeron=1
	fi
}

function printWatcherHeader(){
	local headers=($idxlagheader)
	local stats=$(echo "$1" | wc -w)
	if [ "$headeron" -eq 0 ]; then
		let firstcollen=firstcollen+1
		printf "\e[33m\e[4m%-${firstcollen}s\e[0m\e[0m" "Workload"
		local i=0
		for col in ${headers[@]}
		do
			local collen=$idxcolw
			printf "\e[33m\e[4m%${collen}s\e[0m\e[0m" "$col"
			let i=i+1
			[ "$i" -ge "$stats" ] && break
		done
		echo
		headeron=1
	fi
}

function printClientStatsHeader(){
	local headers=($wrkldids)
	if [ "$headeron" -eq 0 ]; then
		printf "\e[33m\e[4m%-${cscolw}s\e[0m\e[0m" "Client ID"
		for col in ${headers[@]}
		do
			printf "\e[33m\e[4m%${cscolw}s\e[0m\e[0m" "$col(s)"
		done
		echo
		headeron=1
	fi
}

function printClientStats(){
	local client=$1
	client=$(echo $1 | cut -d'_' -f3-4 | tr -d '/')
	local vals=($2)
	printf "\e[1m\e[34m%-${cscolw}s\e[0m\e[0m" "$client"
	for val in ${vals[@]}
	do
		printf "%${cscolw}d" "$val"
	done
	echo
}

# @param worklaod id
# @param stats
function printStats(){
	local vals=($2)
	printf "\e[1m\e[34m%-${firstcollen}s\e[0m\e[0m" "$1"
	printf "%${colw}s" "$wrkldthread"
	for val in ${vals[@]}
	do
		local v=${val%.*}
		printf "%${colw}d" "$v"
	done
	local wrkldid2="$1"
	[ -n "$3" ] && wrkldid2="$3"
	local runtime=$(getRunTime "$wrkldid2")
	printf "%${colw}d" "$runtime"
	echo
}

function buildReplLogJSON(){
	[ ! -s "$twfile" ] && return
	local wrkldid=$1

	local wstats="$(getWatcherStats "$wrkldid" "onlyidxts")"
	[ -z "$wstats" ] && return
	
	local startts=
	local endts=
	
	for val in ${wstats[@]}
	do
		local v=$(date -d @$val '+%Y-%m-%d %H:%M:%S')
		[ -z "$startts" ] && startts="$v"
		[ -n "$startts" ] && endts="$v"
	done

	
	local sl=$(grep -n "$startts" "$twfile" | tail -1 | cut -d':' -f1)
	local el=$(grep -n "$endts" "$twfile" | head -1 | cut -d':' -f1)

	local loglines=$(sed -n ${sl},${el}p $twfile)

	local json=
	json=$json"\"repllist\":"
	local repllistjson="["
	local repllist=$(echo "$loglines"| head -1 | tr ' ' '\n' | grep -v "=" | grep "\[" | tr -d '[')
	for repl in $repllist
	do
		repllistjson=$repllistjson"\"$repl\","
	done
	repllistjson=$(echo $repllistjson | sed 's/,$//')
	repllistjson=$repllistjson"]"
	repllistjson=$(echo $repllistjson | python -c 'import json,sys; print json.dumps(sys.stdin.read())')
	json=$json"$repllistjson,"
	
	local logjson="["
	local timecnt=0
	while read -r logline 
	do
		logjson=$logjson"{\"time\":$timecnt"
		local ts=$(echo "$logline" | awk '{print $1, $2}')
		local logts=$(date -d "$ts" +%s)
		logjson=$logjson",\"ts\":$logts"
		repllist=$(echo "$logline" | grep -o latency=[0-9]* | cut -d'=' -f2 | sed ':a;N;$!ba;s/\n/,/g')
		local winlag=$(echo "$logline" | grep -o windowpending=[0-9]* | cut -d'=' -f2 | sed ':a;N;$!ba;s/\n/,/g')
		logjson=$logjson",\"lat\":[$repllist],\"winpen\":[$winlag]},"
		let timecnt=timecnt+1
	done <<< "$loglines"

	logjson=$(echo $logjson | sed 's/,$//')
	logjson=$logjson"]"
	logjson=$(echo $logjson | python -c 'import json,sys; print json.dumps(sys.stdin.read())')
	json=$json"\"repllog\":""$logjson"

	echo "$json"
}

# @param workload id
# @param workload type
function buildRunLogJSON (){
	#echo "Starting to build chart data"
	local wid=$1
	local wtype=$2
	local postidx=$3
	local newver=$(find . -type f -name ${wid}_out.log | xargs cat  2>/dev/null | grep operations | head -n 100 | grep completion)
	local opslist=
	if [ -n "$newver" ]; then
		opslist=$(find . -type f -name ${wid}_out.log | xargs cat | grep operations | grep -v Spike | grep -v CLEANUP | sed -e 's/est completion[^[]*\[/[/g' | sed -e 's/est completion.*//' | sed -e 's/MinLatency(us)=[0.9]*//g' | sed -e 's/MaxLatency(us)=[^0.9]*.*]//g' | awk '{if ($8=="current" && NF>11) print $1,$2,$5,$5/$3,$7,$11,$13,$15; else if($3>0 && $7>0 && $8=="current") print $1,$2,$5,$5/$3,$7,$11,$13,$15; else if($3>0 && $5>0) print $1,$2,$5,$5/$3,$7,$7,$7,$7; else if($3==0) print $1,$2,$5,$5,$5,$5,$5,$5}' | sort -k1 -k2 | sed 's/AverageLatency(us)=//g' | sed 's/\(.*\):/\1 /' | tr -d ']' | awk '{ts=$1" "$2; cnt[ts]+=1; opscnt[ts]+=$4; runrate[ts]+=$5; oprate[ts]+=$6; avglat1[ts]+=$7; avglat2[ts]+=$8;} END {for (i in cnt) { print i,opscnt[i],runrate[i]/cnt[i],oprate[i]/cnt[i],avglat1[i]/cnt[i],avglat2[i]/cnt[i] } }' | sort -k1 -k2)
	else
		opslist=$(find . -type f -name ${wid}_out.log | xargs cat | grep operations| grep -v Spike | grep -v CLEANUP | awk '{if ($12=="current" && NF>13) print $1,$2,$3,$4,$5,$6,$9,$9/$7,$11,$15,$21,$27; else if($7>0 && $11>0 && $12=="current") print $1,$2,$3,$4,$5,$6,$9,$9/$7,$11,$12,$18,$24; else if($7>0 && $9>0) print $1,$2,$3,$4,$5,$6,$9,$9/$7,$11,$11,$11,$11; else if($7==0) print $1,$2,$3,$4,$5,$6,$9,$9,$9,$9,$9,$9}' | sort -k2M -k3 -k4 | sed 's/AverageLatency(us)=//g' | awk '{ts=$1" "$2" "$3" "$4" "$5" "$6; cnt[ts]+=1; opscnt[ts]+=$7; runrate[ts]+=$8; oprate[ts]+=$9; avglat1[ts]+=$10; avglat2[ts]+=$11;} END {for (i in cnt) { print i,opscnt[i],runrate[i]/cnt[i],oprate[i]/cnt[i],avglat1[i]/cnt[i],avglat2[i]/cnt[i] } }' | sort -k2M -k3 -k4)
	fi

	local json="runlog={"
	json=$json"\"timestamp\":""$TIMESTAMP,"
	json=$json"\"wrkldid\":""\"$wid\","
	json=$json"\"wrkldtype\":""\"$wtype\","
	json=$json"\"id\":""$postidx,"

	local logjson="["
	local timecnt=0
	
	while read -r ops || [ -n "$ops" ]; 
	do
		ops=($ops)
		local ts=
		if [ -z "$newver" ]; then
			ts=$(date -d "${ops[0]} ${ops[1]} ${ops[2]} ${ops[3]} ${ops[4]} ${ops[5]}" +%s)
		else
			ts=$(date -d "${ops[0]} ${ops[1]}" +%s)
		fi
		
		# generate JSON here
		if [[ "$timecnt" -gt 0 ]]; then
			logjson=$logjson",{"
		else
			logjson=$logjson"{"
		fi
		
		local opscnt=
		local runrate=
		local oprate=
		local avglat1=
		local avglat2=

		if [ -z "$newver" ]; then
			opscnt=${ops[6]}
			runrate=${ops[7]}
			oprate=${ops[8]}
			avglat1=${ops[9]}
			avglat2=${ops[10]}
		else
			opscnt=${ops[2]}
			runrate=${ops[3]}
			oprate=${ops[4]}
			avglat1=${ops[5]}
			avglat2=${ops[6]}
		fi
		
		logjson=$logjson"\"time\":$timecnt,"
		logjson=$logjson"\"opscount\":$(printf "%.0f" $opscnt),"
		logjson=$logjson"\"opsrate\":$(printf "%.0f" $oprate),"
		logjson=$logjson"\"runrate\":$(printf "%.0f" $runrate),"
		if [ "$wtype" != "MIXED" ]; then
			logjson=$logjson"\"avglat\":$(printf "%.0f" $avglat1)"
		else
			logjson=$logjson"\"ravglat\":$(printf "%.0f" $avglat1),"
			logjson=$logjson"\"wavglat\":$(printf "%.0f" $avglat2)"
		fi
		logjson=$logjson"}"

		let timecnt=timecnt+1
	done <<< "$opslist"
	logjson=$logjson"]"
	logjson=$(echo $logjson | python -c 'import json,sys; print json.dumps(sys.stdin.read())')
	json=$json"\"log\":""$logjson"
	local repljson=$(buildReplLogJSON "$wid")
	[ -n "$repljson" ] && json=$json",$repljson"
	json=$json"}"
	echo "$json"
}

function postRunInfoToPuffd()
{
	local workload=$(cat $wrkldfile | python -c 'import json,sys; print json.dumps(sys.stdin.read())')
	local maxtbls=$(grep maxtables: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
	local startbtls=$(grep starttable: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
	local maprdisks=0
	local nummfs=0
	local disktype="HDD"
	if [ -e "/opt/mapr/conf/disktab" ]; then
		maprdisks=$(cat /opt/mapr/conf/disktab | grep -v MapR | grep -v -e '^$' | cut -d' ' -f1 | cut -d'/' -f3)
		nummfs=$(/opt/mapr/server/mrconfig info instances | head -1)
		local hddcnt=0
		local ssdcnt=0
		
		for i in $maprdisks
		do
			local type=$(cat /sys/block/$i/queue/rotational)
			if [ "$type" -eq 0 ]; then
				let ssdcnt=ssdcnt+1
			elif [ "$type" -eq 1 ]; then
				let hddcnt=hddcnt+1
			fi
		done

		if [ "$(echo "$maprdisks" | wc -l)" -eq "$ssdcnt" ]; then
			disktype="SSD"
		elif [ "$hddcnt" -gt 0 ] && [ "$ssdcnt" -gt 0 ]; then
			disktype="HDD & SSD"
		fi
	fi
	
	if [ -n "$startbtls" ] && [ -n "$maxtbls" ] && [ "$maxtbls" -gt 1 ]; then
		numtables=$(echo "$maxtbls-$startbtls"|bc)
	fi
	
	local json="runinfo={"
	json=$json"\"timestamp\":""$TIMESTAMP,"
	json=$json"\"os\":""\"$os\","
	json=$json"\"build\":""\"$build\","
	json=$json"\"driver\":""\"$driver\","
	json=$json"\"totalcpus\":""$cpus,"
	json=$json"\"totalmemory\":""$mem,"
	json=$json"\"disktype\":""\"$disktype\","
	json=$json"\"numdisks\":""$(echo "$maprdisks" | wc -l),"
	json=$json"\"nummfs\":""$nummfs,"
	json=$json"\"totalspace\":""$diskspace,"
	json=$json"\"numclients\":""$clientcnt,"
	json=$json"\"numnodes\":""$nodecnt,"

	json=$json"\"tabletype\":""\"$wrkldtabletype\","
	json=$json"\"numtables\":""$numtables,"
	json=$json"\"numregions\":""$numregions,"
	json=$json"\"datasize\":""$datasize,"
	json=$json"\"rowcount\":""$numrows,"
	json=$json"\"rowsize\":""\"$rowsize\","
	json=$json"\"network\":""\"$nwspeed\","
	if [ -n "$DESC" ]; then
		json=$json"\"description\":""\"$DESC\","
	else
		json=$json"\"description\":""\"${wrkldtabletype}-$(echo $rowsize | tr -d ')' | tr -d '(')-"$(echo "$datasize/1073741824"|bc)"GB-${nodecnt}Sx${clientcnt}C\","
	fi

	[ -n "$wrkldreplica" ] && json=$json"\"repltype\":""\"$wrkldreplica\","
	json=$json"\"workload\":""$workload"

	json=$json"}"

	postToPuffD "$json"
}

function printPuffDLink(){
	echo "View Results -> http://dash.perf.lab/puffd/ycsb/?timestamp=${TIMESTAMP}"
}

function postToPuffD(){
	local json="$1"
	local devopsjson=$(echo "$1" | sed 's/rundata=//g' | sed 's/runinfo=//g' | sed 's/rundata=//g')
	#echo "JSON to POST -> $json"
	curl -L -X POST --data-urlencode ''"$json" http://dash.perf.lab/puffd/ > /dev/null 2>&1
	curl -H 'Content-Type: vnd/mapr.test-portal.perf-db-test-result-summary+json;v=1.0.0' -vX POST --data-urlencode ''"$devopsjson" http://testing.devops.lab/test-results/result > /dev/null 2>&1
}

function postToPuffD2(){
	local json="$1"
	local devopsjson=$(echo "$1" | sed 's/rundata=//g' | sed 's/runinfo=//g' | sed 's/rundata=//g')
	#echo "JSON to POST -> $json"
	local tmpfile=$(mktemp)
	echo "$json" > $tmpfile
	curl -L -X POST --data @- http://dash.perf.lab/puffd/ < $tmpfile > /dev/null 2>&1
	echo "$devopsjson" > $tmpfile
	curl -H 'Content-Type: vnd/mapr.test-portal.perf-db-test-result-summary+json;v=1.0.0' -vX POST --data @- http://testing.devops.lab/test-results/result < $tmpfile > /dev/null 2>&1
	#> /dev/null 2>&1
	rm -f $tmpfile
}

function buildRunDataJSON(){
	local nargs=$#
	if [ "$nargs" -gt 4 ]; then
		echo "Max number of arguments supported is 4"
		return
	fi

	local json="rundata={"
	local rw="r"
	if [ "$2" == "LOAD" ] || [ "$2" == "DELETE" ]; then
		rw="w"
	fi

	json=$json"\"timestamp\":""$TIMESTAMP,"
	json=$json"\"wrkldid\":""\"$1\","
	json=$json"\"wrkldtype\":""\"$2\","
	json=$json"\"threads\":""$3,"
	json=$json"\"id\":""$postcnt,"
	
	local stats=($4)
	local i=0
	for stat in ${stats[@]}
	do
		case $i in 
			0)
				json=$json"\"throughput\":""$stat,"
			;;
			1)
				json=$json"\"${rw}avg\":""$stat,"
			;;
			2)
				json=$json"\"${rw}min\":""$stat,"
			;;
			3)
				json=$json"\"${rw}max\":""$stat,"
			;;
			4)
				json=$json"\"${rw}p95\":""$stat,"
			;;
			5)
				json=$json"\"${rw}p99\":""$stat,"
			;;
		esac
		let i=i+1 
	done
	local wstats="$(getWatcherStats "$wrkld" "onlystats")"
	i=0
	for stat in ${wstats[@]}
	do
		case $i in 
			0)
				json=$json"\"wrkldtime\":""$stat,"
			;;
			1)
				json=$json"\"repltime\":""$stat,"
			;;
			2)
				json=$json"\"replavg\":""$stat,"
			;;
			3)
				json=$json"\"replmax\":""$stat,"
			;;
		esac
		let i=i+1 
	done

	json=$(echo $json | sed 's/,$//')
	json=$json"}"
	echo "$json"
}

function buildRunDataJSON2(){
	local nargs=$#
	if [ "$nargs" -gt 7 ]; then
		echo "Max number of arguments supported is 6"
		return
	fi

	local json="rundata={"
	json=$json"\"timestamp\":""$TIMESTAMP,"
	json=$json"\"wrkldid\":""\"$1\","
	json=$json"\"wrkldtype\":""\"$2\","
	json=$json"\"id\":""$postcnt,"
	json=$json"\"threads\":""$3"
	
	local i=0
	while [ "$4" != "" ]; do
		local stats=($4)
		local rw="w"
		if [ "$i" -eq 1 ]; then
			rw="r"
		fi
		local j=0
		for stat in ${stats[@]}
		do
			if [ "${#stats[@]}" -gt 1 ] && [ "$j" -eq 0 ]; then
				json=$json","
			fi
			case $j in 
				0)
					if [ "$i" -eq 0 ]; then
						json=$json"\"throughput\":""$stat,"
					fi
				;;
				1)
					json=$json"\"${rw}avg\":""$stat,"
				;;
				2)
					json=$json"\"${rw}min\":""$stat,"
				;;
				3)
					json=$json"\"${rw}max\":""$stat,"
				;;
				4)
					json=$json"\"${rw}p95\":""$stat,"
				;;
				5)
					json=$json"\"${rw}p99\":""$stat"
				;;
			esac
			let j=j+1 
		done
		let i=i+1 
		shift
	done
	json=$json","
	local wstats="$(getWatcherStats "$wrkld" "onlystats")"
	i=0
	for stat in ${wstats[@]}
	do
		case $i in 
			0)
				json=$json"\"wrkldtime\":""$stat,"
			;;
			1)
				json=$json"\"repltime\":""$stat,"
			;;
			2)
				json=$json"\"replavg\":""$stat,"
			;;
			3)
				json=$json"\"replmax\":""$stat,"
			;;
		esac
		let i=i+1 
	done
	json=$(echo $json | sed 's/,$//')
	json=$json"}"
	echo "$json"
}

function postRunLogToPuffd2(){
	local p1=$1
	local p2=$2
	local p3=$3
	local json=
	case $p2 in
	 	load)
			json=$(buildRunLogJSON "$p1" "LOAD" "$p3")
			;;
		read)
			json=$(buildRunLogJSON "$p1" "READ" "$p3")
			;;
		scan)
			json=$(buildRunLogJSON "$p1" "SCAN" "$p3")
			;;
		delete)
			json=$(buildRunLogJSON "$p1" "DELETE" "$p3")
			;;
		mixed)
			json=$(buildRunLogJSON "$p1" "MIXED" "$p3")
			echo
			;;
    esac
    if [ -n "$json" ]; then
    	echo -e "\t Posting workload $1"
    	#echo "$json" > /tmp/$1
    	postToPuffD2 "$json"
    fi
}

function postRunLogToPuffd(){
	local i=0
	echo "Posting run logs to puffd"
	for wrkld in ${wrkldids[@]}
	do
		local wrktype=${wrkldtypes[$i]}
		postRunLogToPuffd2 "$wrkld" "$wrktype" "$i" &
	    let i=i+1
	done
	wait
}

function postRunDataToPuffd(){
	i=0
	for wrkld in ${wrkldids[@]}
	do
		local json=
		local wrkldtype=${wrkldtypes[$i]}
		local wrkldthread=${wrkldthreads[i]}
		let postcnt=postcnt+1
		case $wrkldtype in
		 	load)
				local stats="${resultarr[$i,0]}"
				json=$(buildRunDataJSON "$wrkld" "LOAD" "$wrkldthread" "$stats")
				;;
			read)
				local stats="${resultarr[$i,0]}"
				json=$(buildRunDataJSON "$wrkld" "READ" "$wrkldthread" "$stats")
				;;
			scan)
				local stats="${resultarr[$i,0]}"
				json=$(buildRunDataJSON "$wrkld" "SCAN" "$wrkldthread" "$stats")
				;;
			delete)
				local stats="${resultarr[$i,0]}"
				json=$(buildRunDataJSON "$wrkld" "DELETE" "$wrkldthread" "$stats")
				;;
			mixed)
				local updatestats="${resultarr[$i,0]}"
				local readstats="${resultarr[$i,1]}"
				local loadstats="${resultarr[$i,2]}"
				local rmwstats="${resultarr[$i,3]}"
				json=$(buildRunDataJSON2 "$wrkld" "MIXED" "$wrkldthread" "$updatestats" "$readstats" "$loadstats" "$rmwstats")
				#json=$(buildRunDataJSON2 "$wrkld" "MIXED" "$wrkldthread" "$updatestats" "$readstats" "$loadstats")
				echo
				;;
	    esac
	    if [ -n "$json" ]; then
	    	postToPuffD "$json"
		[ -z "$done" ] && printPuffDLink && done=1
	    fi
	    let i=i+1
	done
}

# @param workload id
# @param workload type
function getStats(){
	local throughput=$(getThroughput "$1")
	local avgLat=$(getAvgLatency "$1" "$2")
	local minLat=$(getMinLatency "$1" "$2")
	local maxLat=$(getMaxLatency "$1" "$2")
	local lat95pctile=$(get95thPercentileLatency "$1" "$2")
	local lat99pctile=$(get99thPercentileLatency "$1" "$2")
	echo "$throughput $avgLat $minLat $maxLat $lat95pctile $lat99pctile"

}

function getIndexStartTime(){
	[ ! -s "$twfile" ] && return
	local wrkldstart=$1
	local wrkldend=$2
	local startts=$(cat $twfile | grep 'STARTED' | awk '{print $1,$2}')
	local istartts=
	while read -r ts; do
		local logts=$(date -d "$ts" +%s)
		if [ "$logts" -ge "$wrkldstart" ] && [ "$logts" -le "$wrkldend" ]; then
			istartts=$logts
			break
		fi
	done <<< "$startts"
	echo "$istartts"
}

function getIndexEndTime(){
	[ -z "$1" ] && return
	local istartts=$(date -d @$1 +'%Y-%m-%d %H:%M:%S')
	local endts=$(cat $twfile | grep -B1 'STARTED\|COMPLETED' | grep -A2 "$istartts" | grep bucketspending | awk '{print $1,$2}')
	local iendts=$(date -d "$endts" +%s)
	echo "$iendts"
}

function getIndexTimeTaken(){
	[ -z "$1" ] && return
	local iendts=$(date -d @$1 +'%Y-%m-%d %H:%M:%S')
	local timetaken=$(cat $twfile | grep -A1 "$iendts" | grep COMPLETED | grep -o "time=[0-9]*" | cut -d'=' -f2 | sort -nr | head -1)
	echo $timetaken
}

function getIndexAvgLat(){
	[ -z "$1" ] && return
	local iendts=$(date -d @$1 +'%Y-%m-%d %H:%M:%S')
	local avglat=$(cat $twfile | grep -A1 "$iendts" | grep COMPLETED | grep -o "AvgLatency=[0-9]*" | cut -d'=' -f2 | sort -nr | head -1)
	echo $avglat
}

function getIndexMaxLat(){
	[ -z "$1" ] && return
	local iendts=$(date -d @$1 +'%Y-%m-%d %H:%M:%S')
	local maxlat=$(cat $twfile | grep -A1 "$iendts" | grep COMPLETED | grep -o "MaxLatency=[0-9]*" | tr -d ']' | cut -d'=' -f2 | sort -nr | head -1)
	echo $maxlat
}

function getWatcherStats(){
	local startts=$(getStartTime "$1")
	local endts=$(getEndTime "$1")
	local tsdiff=$(echo "$endts-$startts" | bc)
	
	local istartts=$(getIndexStartTime "$startts" "$endts")
	local iendts=$(getIndexEndTime "$istartts")
	local repdiff=$(getIndexTimeTaken "$iendts")
	local itsdiff=
	[ -n "$iendts" ] && itsdiff=$(echo "$iendts-$istartts" | bc)
	[ -n "$repdiff" ] && [ "$itsdiff" -lt "$repdiff" ] && itsdiff=$repdiff
	local avglat=$(getIndexAvgLat "$iendts")
	local maxlat=$(getIndexMaxLat "$iendts")
	if [ -z "$2" ]; then
		echo "$startts $endts $tsdiff $istartts $iendts $itsdiff $avglat $maxlat"
	elif [ "$2" = "onlyidxts" ]; then
		[ -n "$istartts" ] && echo "$istartts $iendts"
	else
		[ -n "$itsdiff" ] && echo "$tsdiff $itsdiff $avglat $maxlat"
	fi
}

function getClientStats(){
	local wrklds=($1)
	local client=$2
	local stats=
	for wrkld in ${wrklds[@]}
	do
		local startts=$(getStartTime "$wrkld" "$client")
		local endts=$(getEndTime "$wrkld" "$client")
		local tsdiff=$(echo "$endts-$startts" | bc)
		[ -z "$tsdiff" ] && tsdiff=0
		stats="$stats $tsdiff"
	done
	echo "$stats"
}

function printWatcherStats(){
	local wrkld=$1
	local vals=("$2")
	printf "\e[1m\e[34m%-${firstcollen}s\e[0m\e[0m" "$1"
	local i=1
	for val in ${vals[@]}
	do
		local v="$val"
		#${val%.*}
		if [ "$i" -eq "3" ] || [ "$i" -ge "6" ] ; then
			printf "%${idxcolw}d" "$v"
		else
			v=$(date -d @$v +%H:%M:%S)
			printf "%${idxcolw}s" "$v"
		fi
		let i=i+1
	done
	echo
}

function printAllClientStats(){
	i=0
	headeron=0
	local clientlist=$(ls -d ycsb_*/)
	for client in ${clientlist[@]}
	do
		local stats="$(getClientStats "$wrkldids" "$client")"
		printClientStatsHeader "$wrkldids"
		printClientStats "$client" "$stats"
	done
	echo
}

function parseWatcherLogs() {
	i=0
	headeron=0
	for wrkld in ${wrkldids[@]}
	do
		local stats="$(getWatcherStats "$wrkld")"
		printWatcherHeader "$stats"
		printWatcherStats "$wrkld" "$stats"
	done
	echo
}

function parseLogs() {
	i=0
	for wrkld in ${wrkldids[@]}
	do
		local stats=
		wrkldtype=${wrkldtypes[$i]}
		wrkldthread=${wrkldthreads[i]}
		printHeader
		 case $wrkldtype in
		 	load)
				stats=$(getStats "$wrkld" "INSERT")
				printStats "$wrkld" "$stats"
				resultarr[$i,0]="$stats"
				;;
			read)
				stats=$(getStats "$wrkld" "READ")
				printStats "$wrkld" "$stats"
				resultarr[$i,0]="$stats"
				;;
			scan)
				stats=$(getStats "$wrkld" "SCAN")
				printStats "$wrkld" "$stats"
				resultarr[$i,0]="$stats"
				;;
			delete)
				stats=$(getStats "$wrkld" "DELETE")
				printStats "$wrkld" "$stats"
				resultarr[$i,0]="$stats"
				;;
			mixed)
				local mixedloadstats=$(getStats $wrkld "INSERT")
				local mixedupdatestats=$(getStats $wrkld "UPDATE")
				local mixedreadstats=$(getStats $wrkld "\[READ\]" )
				local mixedrmwstats=$(getStats $wrkld "READ-MODIFY-WRITE" )
				local inserts=($mixedloadstats)
				echo -e "\e[1m\e[34m$wrkld\e[0m\e[0m"
				if [ "${#inserts[@]}" -gt 1 ]; then
					printStats "   INSERT" "$mixedloadstats" "$wrkld"
				fi
				if [ "${#mixedupdatestats[@]}" -gt 0 ]; then
					printStats "   update" "$mixedupdatestats" "$wrkld"
				fi
				if [ "${#mixedreadstats[@]}" -gt 0 ]; then
					printStats "   read" "$mixedreadstats" "$wrkld"
				fi
				if [ "${#mixedrmwstats[@]}" -gt 1 ]; then
					printStats "   r-m-w" "$mixedrmwstats" "$wrkld"
				else
					mixedrmwstats=
				fi
				resultarr[$i,0]="$mixedupdatestats"
				resultarr[$i,1]="$mixedreadstats"
				resultarr[$i,2]="$mixedloadstats"
				resultarr[$i,3]="$mixedrmwstats"
				;;
			*)
	            echo "{ERROR} : WRONG workload id !?!"
	            exit 1
	            ;;
	    esac
	    shift
		let i=i+1
	done
	echo
	parseWatcherLogs && doWatch=0
	printAllClientStats && doClientStats=0
	printRunInfo
	printClusterInfo
	if [ "$doPost" -eq 1 ]; then
		postRunInfoToPuffd
		postRunDataToPuffd
		[[ "$doPlot" -eq 1 ]] && postRunLogToPuffd
	fi
}

function graphLogs (){
	echo "Starting to publish metrics to graphite [$GRAPHITEHOST]"
	for wrkld in ${wrkldids[@]}
	do
		echo -e "\tPublishing metrics to graphite [$wrkld] "
		local loglist=$(find . -name ${wrkld}_out.log)
		for log in $loglist
		do
			local host=$(echo $log | cut -d'/' -f2 | cut -d'_' -f3-)
			host=${host//./_}
			local opslines=$(cat $log | grep operations | grep -v Spike | awk '{if ($11>0) print $1,$2,$3,$4,$5,$6,$9,$11,$9/$7}')
			printf %s "$opslines" | while IFS= read -r ops; do
			ops=($ops)
			local ts=$(date -d "${ops[0]} ${ops[1]} ${ops[2]} ${ops[3]} ${ops[4]} ${ops[5]}" +%s)
			local opscnt=${ops[6]}
			local oprate=${ops[7]}
			local runrate=${ops[8]}
			echo "ycsb.$TIMESTAMP.$host.$wrkld.opsrate $oprate $ts" | nc $GRAPHITEHOST $GRAPHITEPORT
			echo "ycsb.$TIMESTAMP.$host.$wrkld.opscount $opscnt $ts" | nc $GRAPHITEHOST $GRAPHITEPORT
			echo "ycsb.$TIMESTAMP.$host.$wrkld.runrate $runrate $ts" | nc $GRAPHITEHOST $GRAPHITEPORT
			done
		done
	done
	echo "Completed"
	#set +x
}

function analyzeLogs(){
	echo "Starting to analyze logs"

	echo "Checking all workload logs for errors, exceptions, Full GC ..."
	for wrkld in ${wrkldids[@]}
	do
		echo -e "\tWorkload [$wrkld] "
		grepWorkloadLog "$wrkld" "ERROR"
		grepWorkloadLog "$wrkld" "Exception"
		grepWorkloadStatusLog "$wrkld"
		grepWorkloadGCLog "$wrkld" "Full GC"
	done
	
	echo "Checking all FATAL errors in mfs logs... "
	grepMFSLogs "FATAL"

	echo "Checking for disk errors... "
	grepMFSLogs "DHL" "lun.cc"

	echo
}

function searchLogs(){
	echo "Search"
	grepAllLogs "$SEARCHKEY"
}

function getWorkload(){
	local ignorelist=(log bin jar index zip gz sh threads lab)
	local findcmd="find $PWD -type f "
	for i in "${ignorelist[@]}"
	do
		findcmd=$findcmd" ! -name *."$i
	done
	findcmd=$findcmd" ! -name \"mfs.log*\""
	#echo $findcmd
	local wrkldfile=$($findcmd | grep -v .tar | xargs grep workloads: 2> /dev/null | head -1 | cut -d":" -f1)
	echo "$wrkldfile"
}

function getTableReplica(){
	local tablepath=$1
	local tabletype=$2
	local replicatype=
	local hasrepl=
	if [ "$tabletype" != "binary" ]; then
		hasrepl=$(maprcli table index list -path $tablepath -json 2> /dev/null | grep bucketsPending)
		[ -n "$hasrepl" ] && replicatype="index"
	fi
	if [ -z "$replicatype" ]; then
		hasrepl=$(maprcli table replica list -path $tablepath -json 2> /dev/null | grep bucketsPending)
		[ -n "$hasrepl" ] && replicatype="replica"
	fi
	if [ -z "$replicatype" ]; then
		hasrepl=$(maprcli table changelog list -path $tablepath -json 2> /dev/null | grep bucketsPending)
		[ -n "$hasrepl" ] && replicatype="changelog"
	fi
	[ -n "$replicatype" ] && echo "$replicatype"
}

function getWorkloadCtx(){
	local wf=$1
	local q=$2
	local tmpwf="/tmp/workload.json"
	local wline=$(grep -nr workloads: $wf | cut -d':' -f1)
	local jline=$(grep -nr java: $wf | cut -d':' -f1)
	sed -n $wline,${jline}p $wf > $tmpwf 
	local wtypes=
	
	wtypes=$(grep type: $tmpwf| tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
	
	local retval=
	for i in $wtypes
	do
		local val=
		local delline=$(echo "$(grep -nr type: $tmpwf| head -1 | cut -d':' -f1)-1"|bc)
		local newc=$(cat $tmpwf | sed -e "1,${delline}d")
		echo "$newc" > $tmpwf

		local wtline=$(grep -nr "\"$i\"" $tmpwf| head -1 | cut -d':' -f1)
		local isenabled=$(tail -n +$wtline $tmpwf | grep -A 5 "\"$i\"" | grep enabled | head -1 | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
		if [ "$isenabled" == "true" ]; then
			if [[ "$q" == "type" ]]; then
				val=$i
			elif [[ "$q" == "id" ]]; then
				val=$(grep id: $tmpwf| head -n 1 | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
			elif [[ "$q" == "threadcount" ]]; then
				val=$(grep threadcount: $tmpwf|  head -n 1 | sed 's/\/.*//g' | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
			fi

			if [ -z "$retval" ]; then
				retval=$val
			else
				retval=$retval" $val"
			fi
		fi
		newc=$(cat $tmpwf | sed -e "1,${wtline}d")
		echo "$newc" > $tmpwf
	done
	echo "$retval"
}

SEARCHKEY=
DESC=
doParse=0
doAnalyze=1
doSearch=0
doForce=0
doPost=0
doWatch=0
doClientStats=0
doPlot=0
doGraph=0


for i in "$@"
do
	OPTION=`echo $i | awk -F= '{print $1}'`
    VALUE=`echo $i | awk -F= '{print $2}'`
    case $OPTION in
    	-p | --parse | parse)
			doParse=1
			;;
		-a | --analyze | analyze)
			doAnalyze=1
			;;
		-y)
			doForce=1
			;;
		-i)
			doWatch=1
			;;
		-c)
			doClientStats=1
			;;
		-g)
			doGraph=1
			;;
		-post)
			doPost=1
			;;
		-plot)
			doPlot=1
			;;
		-s | --search | search)
			if [ -n "$VALUE" ]; then
				SEARCHKEY=$VALUE
				doSearch=1
			else
				echo "No Search keyword specified"
			fi
			;;
		-d | --desc | desc)
			if [ -n "$VALUE" ]; then
				DESC=$VALUE
			fi
			;;
        -h | --help)
            usage
            exit
            ;;
        *)
			echo
			;;
    esac
done

if [ -z "$TIMESTAMP" ]; then
	if [ -n "$caltimestamp" ]; then
		TIMESTAMP=$caltimestamp
		echo "No directory/timestamp specified. Using $logsdir/$TIMESTAMP directory."
		if [[ $doForce -eq 0 ]]; then
		    read -p "Press 'y' to confirm... " -n 1 -r
		    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
		    	echo
		    	echo "Aborted!"
		        exit 1
		    else
		    	echo
		    fi
		fi
	else
		echo "{ERROR} : Please specify the timestamp"
		usage
		exit 1
	fi
else
	if [ ! -d "$logsdir/$TIMESTAMP" ]; then
		echo "{ERROR} Specified directory[$logsdir/$TIMESTAMP] doesn't exist"
		usage
		exit 1
	fi
fi

cd $logsdir/
mkdir $TIMESTAMP > /dev/null 2>&1
direxists=$?

if [ "$direxists" -eq 0 ]; then
	mv ycsb*$TIMESTAMP* $TIMESTAMP/
	[ -s "$twfile" ] && cp $twfile $TIMESTAMP
	cd $TIMESTAMP
	echo "extracting bzip2"
	for i in `ls *.bz2`;do bzip2 -dk $i;done 
	echo "extracting tar"
	for i in `ls *.tar`;do DIR=`echo $i| sed 's/.tar//g'`;echo $DIR;mkdir -p $DIR;tar -xf $i -C `pwd`/$DIR && rm -f ${i};done
	mkdir -p runlog > /dev/null 2>&1
	[ -s "$twfile" ] && mv $twfile runlog/
else
	cd $TIMESTAMP
fi

twfile="runlog/$twfile"
wrkldfile=$(getWorkload)

wrkldline=$(grep -nr workloads: $wrkldfile | cut -d':' -f1)
wrkldtypes=$(getWorkloadCtx "$wrkldfile" "type")
wrkldids=$(getWorkloadCtx "$wrkldfile" "id")
wrkldtc=$(getWorkloadCtx "$wrkldfile" "threadcount")

wrkldtiline=$(grep -nr tableinfo: $wrkldfile | cut -d':' -f1)
wrkldtabletype=$(tail -n +$wrkldtiline $wrkldfile | grep type: | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g' | sed 's/\/.*//g' )
wrkldtable=$(tail -n +$wrkldtiline $wrkldfile | grep table: | tr -d "\"" | tr -d ',' | tr -d " "| cut -d':' -f2)
wrkldreplica=$(getTableReplica "$wrkldtable" "$wrkldtabletype")

numregions=$(grep num_initial_regions: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
newtable=$(grep createtable: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
numtables=1
datasize=$(grep datasize: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
numrows=$(find . -name *out.log | xargs grep -o recordcount=[0-9]* 2> /dev/null | head -n 1 | cut -d':' -f2 | cut -d'=' -f2)
fieldscount=$(grep fieldcount: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
fieldslength=$(grep fieldlength: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
multicf=$(grep multicf: $wrkldfile | sed 's/\/.*//g' | tr -d " " | tr -d "," | cut -d':' -f2 | sed ':a;N;$!ba;s/\n/ /g')
if [ "$multicf" = "true" ]; then
	fieldscount=$(echo "$fieldscount-1"|bc)
fi
if [ "$newtable" = "false" ]; then
	numregions=0
fi

clientcnt=$(ls -d ycsb_*/ | wc -l)

wrkldtypes=($wrkldtypes)
wrkldthreads=($wrkldtc)

for wrkid in ${wrkldids[@]}
do
	wrkidlen=${#wrkid}
	if [ "$wrkidlen" -gt $firstcollen ]; then
		firstcollen=$wrkidlen
	fi
done

if [[ "$doParse" -eq 1 ]]; then
	parseLogs
fi

if [[ "$doWatch" -eq 1 ]]; then
	parseWatcherLogs
fi

if [[ "$doClientStats" -eq 1 ]]; then
	printAllClientStats
fi

if [[ "$doAnalyze" -eq 1 ]]; then
	analyzeLogs
elif [[ "$doSearch" -eq 1 ]]; then
	searchLogs
fi

if [[ "$doGraph" -eq 1 ]]; then
	graphLogs
fi
