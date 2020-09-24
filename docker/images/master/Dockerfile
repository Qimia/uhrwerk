FROM uhrwerk/spark-base:3.0.1-hadoop2.7

USER $NB_USER

COPY master.sh /

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark_logs

EXPOSE 8080 7077 6066

RUN echo 'alias ll="ls -l"' >> ~/.bashrc

COPY uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar /spark/jars/

CMD ["/bin/bash", "/master.sh"]
