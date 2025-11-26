FROM apache/airflow:latest

# Install additional packages if needed
# RUN pip install --no-cache-dir some-additional-package

# Copy configuration files
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Copy DAGs and plugins into the image
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/

# Set proper permissions for airflow user
USER root
RUN mkdir -p ${AIRFLOW_HOME}/logs && \
    chown -R airflow:root ${AIRFLOW_HOME} && \
    chmod -R 755 ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/plugins ${AIRFLOW_HOME}/logs

USER airflow

# Verify that DAGs and plugins were copied correctly
RUN echo "DAGs files:" && find ${AIRFLOW_HOME}/dags -type f | wc -l && \
    echo "Plugins files:" && find ${AIRFLOW_HOME}/plugins -type f | wc -l