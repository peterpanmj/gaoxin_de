FROM apache/airflow:2.10.5-python3.12 AS base
USER root
RUN --mount=from=python_wheelhouse,target=/opt/wheelhouse \
    if [ -f /opt/wheelhouse/java17-linux-x64.tar.gz ]; then \
      (cd /opt/wheelhouse && sha256sum -c java17-linux-x64.tar.gz.sha256) \
      && mkdir -p /opt/java \
      && tar -xzf /opt/wheelhouse/java17-linux-x64.tar.gz -C /opt/java --strip-components=1; \
    else \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) \
        -exec sed -i 's#http://deb.debian.org/#https://deb.debian.org/#g' {} + \
      && apt-get -o Dir::Etc::sourcelist=/etc/apt/sources.list.d/debian.sources \
           -o Dir::Etc::sourceparts=- update \
      && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
      && rm -rf /var/lib/apt/lists/* \
      && ln -s /usr/lib/jvm/java-17-openjdk-amd64 /opt/java; \
    fi \
    && /opt/java/bin/java -version \
    && mkdir -p /opt/project /opt/analytics /opt/data \
    && chown -R airflow:0 /opt/project /opt/analytics /opt/data
ENV JAVA_HOME=/opt/java
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_DRIVER_MEMORY=1g
COPY --chown=airflow:0 infra/airflow/requirements.lock /opt/project/requirements.lock
USER airflow
ARG PIP_NO_INDEX=0
RUN --mount=from=python_wheelhouse,target=/opt/wheelhouse \
    python -m venv /opt/analytics \
    && /opt/analytics/bin/pip install --no-cache-dir --find-links=/opt/wheelhouse \
         -r /opt/project/requirements.lock
# Wheels built on Windows can lose the Unix executable bits on Spark launchers.
RUN task_spark_package=$(/opt/analytics/bin/python -c \
      'from pathlib import Path; import pyspark; print(Path(pyspark.__file__).parent)') \
    && find "$task_spark_package/bin" "$task_spark_package/sbin" \
         -type f ! -name '*.cmd' -exec chmod a+x {} +
COPY --chown=airflow:0 pyproject.toml /opt/project/
COPY --chown=airflow:0 src /opt/project/src
COPY --chown=airflow:0 config /opt/project/config
COPY --chown=airflow:0 samples /opt/project/samples
USER airflow
RUN --mount=from=python_wheelhouse,target=/opt/wheelhouse \
    /opt/analytics/bin/pip install --no-cache-dir --find-links=/opt/wheelhouse \
      --no-deps /opt/project
WORKDIR /opt/project

# Disposable test runner with the same Java/Python/application runtime as Airflow.
FROM base AS test
COPY --chown=airflow:0 infra/airflow/requirements-test.lock /opt/project/requirements-test.lock
RUN --mount=from=python_wheelhouse,target=/opt/wheelhouse \
    /opt/analytics/bin/pip install --no-cache-dir --find-links=/opt/wheelhouse \
      -r /opt/project/requirements-test.lock
COPY --chown=airflow:0 tests /opt/project/tests
COPY --chown=airflow:0 dags /opt/project/dags
ENV PATH="/opt/analytics/bin:${PATH}"
ENV PYSPARK_PYTHON=/opt/analytics/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/analytics/bin/python
# Run test/lint commands directly, without Airflow's database/bootstrap entrypoint.
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Keep the default image free of test dependencies and fixtures.
FROM base AS runtime
