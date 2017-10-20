FROM jupyter/all-spark-notebook

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

# jvm variables
ENV SPARK_JVM_EXTRA_ARGS spark.driver.extraJavaOptions
ENV JVM_EXTRA_ARGS -Xcheck:jni

USER root

RUN echo "$SPARK_DRIVER_EXTRA_CLASSPATH $SRCD_JAR\n$SPARK_EXECUTOR_EXTRA_CLASSPATH $SRCD_JAR"\
          >> /usr/local/spark/conf/spark-defaults.conf

RUN echo "$SPARK_BBLFSH_HOST $BBLFSH_HOST\n$SPARK_BBLFSH_PORT $BBLFSH_PORT"\
	>> /usr/local/spark/conf/spark-defaults.conf
	
RUN echo "$SPARK_JVM_EXTRA_ARGS $JVM_EXTRA_ARGS"\
	>> /usr/local/spark/conf/spark-defaults.conf

COPY ./target/scala-2.11/engine-uber.jar /opt/jars/
COPY ./examples/notebooks/* /home/$NB_USER/
COPY ./python /opt/python-engine/

RUN echo "local" > /opt/python-engine/version.txt
RUN pip install -e /opt/python-engine/

# Install spark progress bar plugin
RUN pip install jupyter-spark
RUN jupyter serverextension enable --py jupyter_spark
RUN jupyter nbextension install --py jupyter_spark
RUN jupyter nbextension enable --py jupyter_spark
RUN jupyter nbextension enable --py widgetsnbextension
