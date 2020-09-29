FROM uhrwerk/spark-base:3.0.1-hadoop2.7

ENV SPARK_HISTORY_SERVER_LOG /spark_logs

COPY history-server.sh /
COPY spark.properties /

EXPOSE 18080

CMD ["/bin/bash", "/history-server.sh"]
