#!/bin/bash
# Script to download MapR latest documentation
# Author: jcrutsinger@mapr.com
# Date: 08012017

cd /usr/local/var/www/htdocs/maprdocs.mapr.com/home
wget -r --no-parent http://maprdocs.mapr.com/home/
sleep 10
apachectl restart
exit 0;
