#!/bin/bash
sleep 15

hdfs dfs -test -d /summary
if [ $? -eq 1 ]; then
    echo "DIR DO NOT EXIST"
    hdfs dfs -mkdir /summary
    echo "DIR CREATED"
fi
hdfs dfs -test -e /complete_data.csv
if [ $? -eq 1 ]; then
    echo "complete_data.csv do not exists, coping..."
    hdfs dfs -put /home/complete_data.csv /complete_data.csv
fi
