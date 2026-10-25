FROM apache/airflow:2.10.5-python3.12
USER root
RUN find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) \
      -exec sed -i 's#http://deb.debian.org/#https://deb.debian.org/#g' {} + \
    && apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/project /opt/analytics /opt/data \
    && chown -R airflow:0 /opt/project /opt/analytics /opt/data
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_DRIVER_MEMORY=1g
COPY --chown=airflow:0 infra/airflow/requirements.lock /opt/project/requirements.lock
USER airflow
RUN python -m venv /opt/analytics && /opt/analytics/bin/pip install --no-cache-dir -r /opt/project/requirements.lock
COPY --chown=airflow:0 pyproject.toml /opt/project/
COPY --chown=airflow:0 src /opt/project/src
COPY --chown=airflow:0 config /opt/project/config
COPY --chown=airflow:0 samples /opt/project/samples
USER airflow
RUN /opt/analytics/bin/pip install --no-cache-dir --no-deps /opt/project
WORKDIR /opt/project
