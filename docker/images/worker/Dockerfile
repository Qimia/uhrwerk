FROM uhrwerk/spark-base:3.0.1-hadoop2.7

COPY worker.sh /

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark_logs
ENV SPARK_MASTER "spark://spark-master:7077"

EXPOSE 8081

RUN echo 'alias ll="ls -l"' >> ~/.bashrc
RUN echo 'alias python="/usr/bin/python3"' >> ~/.bashrc

COPY uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar /spark/jars/

CMD ["/bin/bash", "/worker.sh"]
