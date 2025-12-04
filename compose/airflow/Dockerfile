FROM apache/airflow:latest

# Install additional packages if needed
RUN pip install --no-cache-dir \
    apache-airflow-providers-fab \
    apache-airflow-providers-amazon \
    apache-airflow-providers-http \
    pika

# Copy configuration files
COPY airflow.cfg ./airflow.cfg

# Copy DAGs and plugins into the image
COPY dags/ ./dags/
COPY plugins/ ./plugins/

# Set proper permissions for airflow user
USER root
RUN mkdir -p ${AIRFLOW_HOME}/logs && \
    chown -R airflow:root ${AIRFLOW_HOME} && \
    chmod -R 755 ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/plugins ${AIRFLOW_HOME}/logs

USER airflow

# Set Python path to navigate between DAGs and plugins
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Verify that DAGs and plugins were copied correctly
RUN echo "DAGs files:" && find ${AIRFLOW_HOME}/dags -type f | wc -l && \
    echo "Plugins files:" && find ${AIRFLOW_HOME}/plugins -type f | wc -l