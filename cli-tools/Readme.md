# Uhrwerk's Cli-tools
These are python cli tools which make using Uhrwerk easier from the commandline.

On Ubuntu and Debian they require the following packages to be installed
*  python3-yaml
*  python3-pystache
*  python3-pymysql


## The tools

**uhrwerk-start.py**: A wrapper around spark-submit which helps starting an Uhrwerk job

**uhrwerk-partitions-table.py**: A script to check which partitions have been written for a table