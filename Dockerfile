FROM jupyter/all-spark-notebook

RUN mkdir -p /opt/
ENV SRCD_JAR = /opt/jars/spark-api-uber.jar

USER root

RUN echo "spark.driver.extraClassPath $SRCD_JAR\nspark.executor.extraClassPath $SRCD_JAR"\
          > /usr/local/spark/conf/spark-defaults.conf

COPY ./target/scala-2.11/spark-api-uber.jar /opt/jars/
COPY ./examples/* /home/$NB_USER/
COPY ./python /opt/python-spark-api/

RUN echo "local" > /opt/python-spark-api/version.txt
RUN pip install -e /opt/python-spark-api/