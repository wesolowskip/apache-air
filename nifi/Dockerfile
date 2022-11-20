FROM apache/nifi

COPY flow_setup.sh flow_setup.sh
COPY start.sh start.sh

COPY hdfs-site.xml /opt/nifi/nifi-current/hdfs-site.xml
COPY core-site.xml /opt/nifi/nifi-current/core-site.xml

ENTRYPOINT ["./start.sh"]
