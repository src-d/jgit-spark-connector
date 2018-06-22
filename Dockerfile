FROM openjdk:8-jdk as builder
RUN apt-get update && apt-get install -y --no-install-recommends git
RUN mkdir /engine
WORKDIR /engine
COPY . /engine
RUN ./sbt assembly

FROM srcd/jupyter-spark:5.2.1

RUN mkdir -p /opt/

# engine jar location
ENV SPARK_DRIVER_EXTRA_CLASSPATH spark.driver.extraClassPath
ENV SPARK_EXECUTOR_EXTRA_CLASSPATH spark.executor.extraClassPath
ENV SRCD_JAR /opt/jars/engine-uber.jar

# bblfsh endpoint variables
ENV SPARK_BBLFSH_HOST spark.tech.sourced.bblfsh.grpc.host
ENV BBLFSH_HOST bblfshd
ENV SPARK_BBLFSH_PORT spark.tech.sourced.bblfsh.grpc.port
ENV BBLFSH_PORT 9432

USER root

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends locales curl g++ libxml2-dev && \
    apt-get clean && \
    locale-gen en_US.UTF-8

ENV LANG en_US.UTF-8

COPY ./python /opt/python-engine/
COPY ./_examples/notebooks/* /home/$NB_USER/
COPY --from=builder /engine/target/engine-uber.jar /opt/jars/


RUN echo "local" > /opt/python-engine/version.txt \
    && pip install -e /opt/python-engine/ \
    && pip install jupyter-spark \
    && jupyter serverextension enable --py jupyter_spark \
    && jupyter nbextension install --py jupyter_spark \
    && jupyter nbextension enable --py jupyter_spark \
    && jupyter nbextension enable --py widgetsnbextension

# Separate the config file in a different RUN creation as this may change more often
RUN echo "$SPARK_DRIVER_EXTRA_CLASSPATH $SRCD_JAR\n$SPARK_EXECUTOR_EXTRA_CLASSPATH $SRCD_JAR" >> /usr/local/spark/conf/spark-defaults.conf \
    && echo "$SPARK_BBLFSH_HOST $BBLFSH_HOST\n$SPARK_BBLFSH_PORT $BBLFSH_PORT" >> /usr/local/spark/conf/spark-defaults.conf

# Disable jupyter token
RUN mkdir -p /root/.jupyter && \
    echo "c.NotebookApp.token = ''" > ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.notebook_dir = '/home'" >> ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8080" >> ~/.jupyter/jupyter_notebook_config.py

