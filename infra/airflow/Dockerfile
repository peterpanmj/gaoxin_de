FROM apache/airflow:2.10.5-python3.12
USER root
RUN mkdir -p /opt/project /opt/analytics /opt/data && chown -R airflow:0 /opt/project /opt/analytics /opt/data
COPY --chown=airflow:0 infra/airflow/requirements.lock /opt/project/requirements.lock
USER airflow
RUN python -m venv /opt/analytics && /opt/analytics/bin/pip install --no-cache-dir -r /opt/project/requirements.lock
COPY --chown=airflow:0 pyproject.toml /opt/project/
COPY --chown=airflow:0 src /opt/project/src
COPY --chown=airflow:0 analytics /opt/project/analytics
COPY --chown=airflow:0 config /opt/project/config
COPY --chown=airflow:0 samples /opt/project/samples
USER airflow
RUN /opt/analytics/bin/pip install --no-cache-dir --no-deps /opt/project
WORKDIR /opt/project
