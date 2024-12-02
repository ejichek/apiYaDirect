#!/usr/bin/bash

hdfs dfs -test -e /data/yaDirect/txt_custom_report_1/YaDirect_custum1.txt 

if [ $? -eq 0 ]
then
	hdfs dfs -rm /data/yaDirect/txt_custom_report_1/YaDirect_custum1.txt
	echo "del YaDirect_custum1.txt"
else
	echo "YaDirect_custum1.txt file doesn't exist in the hdfs directory"
fi
