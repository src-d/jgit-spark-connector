FROM srcd/jupyter-spark:5.2.1

ADD build/opt /opt/engine/
ADD samples /opt/engine/samples

# engine jar location
ENV SPARK_DRIVER_EXTRA_CLASSPATH spark.driver.extraClassPath
ENV SPARK_EXECUTOR_EXTRA_CLASSPATH spark.executor.extraClassPath
ENV ENGINE_JAR /opt/engine/lib/engine-uber.jar

# bblfsh endpoint variables
ENV SPARK_BBLFSH_HOST spark.tech.sourced.bblfsh.grpc.host
ENV SPARK_BBLFSH_PORT spark.tech.sourced.bblfsh.grpc.port
ENV BBLFSH_HOST bblfshd
ENV BBLFSH_PORT 9432

USER root

RUN echo "local" > /opt/engine/src/python/version.txt \
    && pip install -e /opt/engine/src/python/ \
    && pip install jupyter-spark \
    && jupyter serverextension enable --py jupyter_spark \
    && jupyter nbextension install --py jupyter_spark \
    && jupyter nbextension enable --py jupyter_spark \
    && jupyter nbextension enable --py widgetsnbextension

# Separate the config file in a different RUN creation as this may change more often
RUN echo "$SPARK_DRIVER_EXTRA_CLASSPATH $ENGINE_JAR\n$SPARK_EXECUTOR_EXTRA_CLASSPATH $ENGINE_JAR" >> /usr/local/spark/conf/spark-defaults.conf \
    && echo "$SPARK_BBLFSH_HOST $BBLFSH_HOST\n$SPARK_BBLFSH_PORT $BBLFSH_PORT" >> /usr/local/spark/conf/spark-defaults.conf

# Jupyter configuration
RUN mkdir -p /root/.jupyter && \
    mkdir -p /opt/engine/workspace && \
    echo "c.NotebookApp.token = ''" > ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.notebook_dir = '/opt/engine/workspace'" >> ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8080" >> ~/.jupyter/jupyter_notebook_config.py
