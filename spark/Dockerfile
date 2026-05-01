FROM tabulario/spark-iceberg

# ── Apache Livy 0.8.0-incubating (Spark 3.x / Scala 2.12) ────────────────────
# Livy is not included in tabulario/spark-iceberg, so we install it here.
# The binary release requires no build — just download, extract, configure,
# and wire its start into the existing entrypoint.

ENV LIVY_VERSION=0.8.0-incubating
ENV LIVY_HOME=/opt/livy
ENV LIVY_CONF_DIR=/opt/livy/conf
ENV SPARK_HOME=/opt/spark

# Install Livy binary release
RUN curl -fsSL \
    "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}-bin.zip" \
    -o /tmp/livy.zip \
  && unzip /tmp/livy.zip -d /opt \
  && mv /opt/apache-livy-${LIVY_VERSION}-bin ${LIVY_HOME} \
  && rm /tmp/livy.zip

# ── Livy configuration ────────────────────────────────────────────────────────
# livy.conf  — tell Livy where Spark lives and allow local file submissions
RUN cp ${LIVY_CONF_DIR}/livy.conf.template ${LIVY_CONF_DIR}/livy.conf && \
    echo "livy.spark.master = local[*]"                             >> ${LIVY_CONF_DIR}/livy.conf && \
    echo "livy.spark.deploy-mode = client"                          >> ${LIVY_CONF_DIR}/livy.conf && \
    echo "livy.file.local-dir-whitelist = /home/iceberg/scripts"    >> ${LIVY_CONF_DIR}/livy.conf && \
    echo "livy.server.port = 8998"                                  >> ${LIVY_CONF_DIR}/livy.conf

# livy-env.sh — point Livy at the Spark installation inside this image
RUN cp ${LIVY_CONF_DIR}/livy-env.sh.template ${LIVY_CONF_DIR}/livy-env.sh && \
    echo "export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f \$(which java))))" >> ${LIVY_CONF_DIR}/livy-env.sh && \
    echo "export SPARK_HOME=${SPARK_HOME}"                                         >> ${LIVY_CONF_DIR}/livy-env.sh && \
    echo "export HADOOP_CONF_DIR="                                                 >> ${LIVY_CONF_DIR}/livy-env.sh

# ── Entrypoint wrapper ────────────────────────────────────────────────────────
# The base image starts Spark services via /bin/start-spark.sh (or similar).
# We wrap it: start Livy in the background first, then exec the original CMD.
COPY start-with-livy.sh /usr/local/bin/start-with-livy.sh
RUN chmod +x /usr/local/bin/start-with-livy.sh

EXPOSE 8998

ENTRYPOINT ["/usr/local/bin/start-with-livy.sh"]
