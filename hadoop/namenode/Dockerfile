FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
COPY complete_data.csv /home/complete_data.csv
COPY init.sh init.sh
COPY init2.sh init2.sh
CMD ["/init2.sh"]
