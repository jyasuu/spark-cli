FROM tabulario/spark-iceberg

# ── Apache Livy 0.8.0-incubating ─────────────────────────────────────────────
# tabulario/spark-iceberg does not include Livy.  We install it here.
# The base image has no ENTRYPOINT — it uses CMD ["/bin/start-spark.sh"].
# We replace CMD with our wrapper which starts Livy, then exec's the original.

ENV LIVY_VERSION=0.8.0-incubating
ENV LIVY_HOME=/opt/livy
ENV SPARK_HOME=/opt/spark

# Install Livy binary release
RUN curl -fsSL \
    "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}-bin.zip" \
    -o /tmp/livy.zip \
  && unzip /tmp/livy.zip -d /opt \
  && mv /opt/apache-livy-${LIVY_VERSION}-bin ${LIVY_HOME} \
  && rm /tmp/livy.zip

# ── Livy configuration ────────────────────────────────────────────────────────
RUN cp ${LIVY_HOME}/conf/livy.conf.template ${LIVY_HOME}/conf/livy.conf \
  && echo "livy.spark.master = local[*]"                           >> ${LIVY_HOME}/conf/livy.conf \
  && echo "livy.spark.deploy-mode = client"                        >> ${LIVY_HOME}/conf/livy.conf \
  && echo "livy.file.local-dir-whitelist = /home/iceberg/scripts"  >> ${LIVY_HOME}/conf/livy.conf \
  && echo "livy.server.port = 8998"                                >> ${LIVY_HOME}/conf/livy.conf

RUN cp ${LIVY_HOME}/conf/livy-env.sh.template ${LIVY_HOME}/conf/livy-env.sh \
  && echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ${LIVY_HOME}/conf/livy-env.sh \
  && echo "export SPARK_HOME=${SPARK_HOME}"                                     >> ${LIVY_HOME}/conf/livy-env.sh \
  && echo "export HADOOP_CONF_DIR="                                             >> ${LIVY_HOME}/conf/livy-env.sh

# ── Entrypoint wrapper ────────────────────────────────────────────────────────
COPY start-with-livy.sh /usr/local/bin/start-with-livy.sh
RUN chmod +x /usr/local/bin/start-with-livy.sh

EXPOSE 8998

# Override CMD (not ENTRYPOINT) to match the base image's convention.
# The base image has no ENTRYPOINT, so CMD is the full command that runs.
CMD ["/usr/local/bin/start-with-livy.sh"]
