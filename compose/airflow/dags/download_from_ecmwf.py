from datetime import datetime, timedelta
import os
from typing import Dict
import requests
import tempfile
from plugins.rabbitmq.hook import RabbitMQHook
from plugins.ecmwf.utils import (
    calculate_ecmwf_params, 
    generate_file_urls
)

from airflow.sdk import DAG, task, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException, AirflowFailException


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='ecmwf_downloader_v1',
    default_args=default_args,
    schedule='0 0,6,12,18 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ecmwf', 's3', 'rabbitmq'],
    doc_md="""
    ## ECMWF Forecast Downloader DAG
    This DAG downloads weather forecast data from ECMWF Open Data service.
    ### Stages:
    - Downloads GRIB2 forecast files at 00, 06, 12, 18 UTC
    - Retry if downloading failed because of HTTP 429
    - Uploads to MinIO/S3 with notifications via RabbitMQ
    ### Configuration:
    Set variables in Airflow UI: Admin -> Variables or env-file
    Key: `ecmwf_downloader_config`
    Value (JSON): '''
    {
        "bucket_name":"...",
        "rabbitmq_exchange":"...",
        "rabbitmq_routing_key":"..."
    }
    '''.
    """
) as dag:
    
    @task
    def get_config() -> Dict[str, str]:
        """
        Fetches the configuration from Airflow Variables.
        Parsed from env variable AIRFLOW_VAR_ECMWF_DOWNLOADER_CONFIG
        """
        config = Variable.get("ecmwf_downloader_config", deserialize_json=True)
        return config

    @task
    def get_target_urls(data_interval_start=None):
        """Generates the list of URLs for the current interval."""
        params = calculate_ecmwf_params(data_interval_start)
        steps = list(range(0, 145, 3)) # 0 to 144 inclusive
        urls = generate_file_urls(params, steps)
        return urls

    @task(max_active_tis_per_dag=2, retries=3, retry_delay=timedelta(minutes=10))
    def download_and_process_file(url: str, config: Dict[str, str]):
        """
        Downloads, uploads to S3, and notifies RabbitMQ using dynamic config.
        """
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        rmq_hook = RabbitMQHook(rabbitmq_conn_id='rabbitmq_default')
        
        bucket_name = config.get("bucket_name")
        exchange = config.get("rabbitmq_exchange")
        routing_key = config.get("rabbitmq_routing_key")

        # Checking for None elements in env variables
        if not bucket_name or not exchange or not routing_key:
            raise AirflowFailException(
                f"AIRFLOW_VAR_ECMWF_DOWNLOADER_CONFIG is incorrect! \
                (no 'bucket_name', 'exchange' or 'routing_key' were provided)"
            )
        
        filename = url.split('/')[-1]
        s3_key = f"ifs/{filename}"
        
        # 1. Download files from ECMWF Open Data
        # Such architecture is more flexible than the official pip package
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 429:
                raise AirflowException(f"Rate limited: {url}")
            if r.status_code == 404:
                raise AirflowFailException(f"Resource not found: {url}")
            r.raise_for_status()
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                    tmp_file.write(chunk)
                tmp_path = tmp_file.name

        try:
            # 2. Upload to S3 (MinIO)
            s3_hook.load_file(
                filename=tmp_path,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            # 3. Notify RabbitMQ
            s3_uri = f"s3://{bucket_name}/{s3_key}"
            rmq_hook.publish(
                exchange=exchange,
                routing_key=routing_key,
                message={"file": s3_uri},
                declare_exchange=False # Passive check for the exchange
            )
            
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # 1. Fetch Config once
    current_config = get_config()
    
    # 2. Generate URLs
    urls = get_target_urls()
    
    # 3. Map the download task
    download_and_process_file.partial(config=current_config).expand(url=urls)