#!/bin/bash
# Created By: Joe Crutsinger
# Date: 01102018
# Hive Load script for demo

# Check to make sure script is ran as mapr
if [[ $USER != "mapr" ]]; then
echo "This script must be run as mapr user."
exit 1
fi

printf "Loading All Data ...\n"
hive -f /mapr/icbc.cluster.com/icbc_data/load_all_data.hql
printf "\n"
printf "Checking Data ...\n"
printf "\n"
printf "Checking Customer Data ...\n"
printf "\n"
hive -e "select * from demo.customers limit 10;"
printf "\n"
printf "Checking Customer Data Record Count ...\n"
hive -e "select count(*) from demo.customers;"
printf "\n"
printf "Checking Claim Data ...\n"
hive -e "select * from demo.claims limit 10;"
printf "\n"
printf "Checking Claim Data Record Count ...\n"
hive -e "select count(*) from demo.claims;"
printf "\n"
printf "Checking Policy Data ...\n"
hive -e "select * from demo.policies limit 10;"
printf "\n"
printf "Checking Policy Data Record Count ...\n"
hive -e "select count(*) from demo.policies;"
printf "\n"
echo "Data Loaded Successfully!"
exit 0;