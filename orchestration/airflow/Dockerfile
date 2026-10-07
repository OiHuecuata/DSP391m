FROM apache/airflow:3.3.2-python3.12 AS job-dependencies
USER root
ENV PIP_USER=false PYTHONNOUSERSITE=1
RUN python -m venv /opt/build-tools \
    && /opt/build-tools/bin/pip install --no-cache-dir poetry==2.5.1 \
    && python -m venv /opt/dsp-python
WORKDIR /tmp/dsp-dependencies
COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    VIRTUAL_ENV=/opt/dsp-python POETRY_VIRTUALENVS_CREATE=false POETRY_INSTALLER_MAX_WORKERS=2 \
    /opt/build-tools/bin/poetry install --only main --no-root --no-interaction

FROM apache/airflow:3.3.2-python3.12
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java17
COPY --from=job-dependencies /opt/dsp-python /opt/dsp-python
ENV JAVA_HOME=/opt/java17 \
    PYSPARK_PYTHON=/opt/dsp-python/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/dsp-python/bin/python
USER airflow
