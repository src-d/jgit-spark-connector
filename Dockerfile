FROM jupyter/all-spark-notebook

COPY ./target/scala-2.11/spark-api-uber.jar /home/$NB_USER/
COPY ./examples /home/$NB_USER/work

ENV SRCD_JAR = /home/$NB_USER/spark-api-uber.jar

USER root

RUN echo "spark.driver.extraClassPath $SRCD_JAR\nspark.executor.extraClassPath $SRCD_JAR"\
          > /usr/local/spark/conf/spark-defaults.conf