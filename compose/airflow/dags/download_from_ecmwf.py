"""
Main DAG for ECMWF Forecast Downloader
"""
from datetime import datetime, timezone
import os
from typing import Dict, List

from airflow.sdk import DAG, task, Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from plugins.rabbitmq.hook import RabbitMQHook
from plugins.ecmwf.downloader import ECMWFDownloader
from plugins.ecmwf.utils import (
    calculate_ecmwf_params,
    generate_file_urls,
    round_robin_distribute
)

def get_default_config() -> Dict:
    """Returns the default configuration. Called inside a task."""
    return {
        # General DAG settings
        "num_workers": 3,
        "steps": list(range(0, 145, 3)),
        "max_active_runs": 1,
        "retries": 2,
        "retry_delay_minutes": 5,

        # MinIO/S3 settings
        "bucket_name": "forecast-data",
        "s3_prefix": "ifs",
        "minio_endpoint": "http://minio:9100",

        # RabbitMQ settings
        "rabbitmq_enabled": True,
        "rabbitmq_conn_id": "rabbitmq_default",
        "rabbitmq_exchange": "forecast_exchange",
        "rabbitmq_routing_key": "opensource_queue",
        "rabbitmq_exchange_type": "direct",
        "rabbitmq_delivery_mode": 2,
        "rabbitmq_priority": 0,
        "rabbitmq_content_type": "application/json",

        # ECMWF settings
        "ecmwf_max_retries": 3,
        "ecmwf_retry_delay": 30,
        "ecmwf_timeout": 60,

        # File processing settings
        "validate_file_size": True,
        "min_file_size_bytes": 1024,
        "cleanup_temp_files": True,
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=timezone.utc),
    'email_on_failure': True,
    'email_on_retry': True,
    'catchup': False,
}

with DAG(
    dag_id='ecmwf_forecast_downloader',
    default_args=default_args,
    description='Download forecast data from ECMWF Open Data to MinIO',
    schedule='0 0,6,12,18 * * *',
    catchup=False,
    tags=['ecmwf', 'forecast', 'minio', 'data-ingestion'],
    doc_md="""
    ## ECMWF Forecast Downloader DAG
    This DAG downloads weather forecast data from ECMWF Open Data service.
    ### Features:
    - Downloads GRIB2 forecast files at 00, 06, 12, 18 UTC
    - Parallel downloading with configurable number of workers
    - Uploads to MinIO/S3 with notifications via RabbitMQ
    - Retry logic with exponential backoff
    ### Configuration:
    Set variables in Airflow UI: Admin -> Variables
    Key: `ecmwf_downloader_config`
    Value (JSON): see the `get_default_config()` function.
    """
) as dag:

    # 0. CONFIGURATION LOADING
    @task(task_id="load_config_and_setup")
    def load_config_and_setup() -> Dict:
        """
        Loads configuration from Airflow Variable.
        This task runs when the DAG is executed, not every time the file is parsed.
        """
        default_config = get_default_config()
        dag_config = Variable.get(
            "ecmwf_downloader_config",
            default=default_config,
            deserialize_json=True
        )
        # Merge default values with user-defined ones
        config = {**default_config, **dag_config}
        print("DAG configuration loaded successfully.")
        return config

    # 1. PREPARE ENDPOINT FOR SENSOR
    @task(task_id="prepare_sensor_endpoint")
    def prepare_sensor_endpoint(data_interval_start: datetime) -> str:
        """Prepares endpoint for HTTP sensor and returns it via XCom."""
        params = calculate_ecmwf_params(data_interval_start)
        return params["base_url"]

    # 2. HTTP SENSOR FOR ECMWF AVAILABILITY CHECK
    check_ecmwf_availability = HttpSensor(
        task_id="check_ecmwf_availability",
        http_conn_id="ecmwf_http",
        # Endpoint is set via Jinja templating, value is pulled from XCom of prepare_sensor_endpoint task
        endpoint="{{ ti.xcom_pull(task_ids='prepare_sensor_endpoint') }}",
        method="HEAD",
        response_check=lambda response: response.status_code == 200,
        poke_interval=60,
        timeout=300,
        mode="poke",
        soft_fail=False,
    )

    # 3. GENERATE DOWNLOAD URLS
    @task(task_id="generate_download_urls")
    def generate_urls(config: Dict, data_interval_start: datetime) -> Dict:
        """Generates all download URLs using configuration."""
        params = calculate_ecmwf_params(data_interval_start)
        steps = config.get("steps", list(range(0, 145, 3)))
        num_workers = config.get("num_workers", 3)

        urls = generate_file_urls(params, steps)
        distributed_urls = round_robin_distribute(urls, num_workers)

        return {
            "params": params,
            "all_urls": urls,
            "distributed_urls": distributed_urls,
            "execution_date_str": data_interval_start.isoformat(),
            "total_files": len(urls)
        }

    # 4. DOWNLOAD AND PROCESS FILES (parallel)
    @task(task_id="download_and_process_files")
    def download_and_process(urls_chunk: List[str], data_params: Dict, config: Dict) -> List[Dict]:
        """
        Downloads a batch of files, saves to MinIO and sends notifications.
        All settings are taken from the config passed to the task.
        """
        # Extract settings from configuration
        ecmwf_max_retries = config.get("ecmwf_max_retries", 3)
        ecmwf_retry_delay = config.get("ecmwf_retry_delay", 30)
        validate_file_size = config.get("validate_file_size", True)
        min_file_size = config.get("min_file_size_bytes", 1024)
        bucket_name = config.get("bucket_name", "forecast-data")
        s3_prefix = config.get("s3_prefix", "ifs")
        minio_endpoint = config.get("minio_endpoint", "http://minio:9100")
        rabbitmq_enabled = config.get("rabbitmq_enabled", True)
        rabbitmq_conn_id = config.get("rabbitmq_conn_id", "rabbitmq_default")
        rabbitmq_exchange = config.get("rabbitmq_exchange", "forecast_exchange")
        rabbitmq_routing_key = config.get("rabbitmq_routing_key", "opensource_queue")
        rabbitmq_exchange_type = config.get("rabbitmq_exchange_type", "direct")
        rabbitmq_delivery_mode = config.get("rabbitmq_delivery_mode", 2)
        rabbitmq_priority = config.get("rabbitmq_priority", 0)
        rabbitmq_content_type = config.get("rabbitmq_content_type", "application/json")
        cleanup_temp_files = config.get("cleanup_temp_files", True)

        results = []
        downloader = ECMWFDownloader(
            max_retries=ecmwf_max_retries,
            retry_delay=ecmwf_retry_delay
        )
        s3_hook = S3Hook(aws_conn_id='minio_conn')

        for url in urls_chunk:
            try:
                filename = url.split("/")[-1]
                temp_path = f"/tmp/{filename}"

                print(f"Downloading: {url}")
                if downloader.download_file(url, temp_path):
                    file_size = os.path.getsize(temp_path)
                    if validate_file_size and file_size < min_file_size:
                        raise Exception(f"File too small: {filename} ({file_size} bytes)")

                    s3_key = f"{s3_prefix}/{filename}"
                    s3_hook.load_file(
                        filename=temp_path,
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )

                    file_url = f"{minio_endpoint}/{bucket_name}/{s3_key}"

                    rabbitmq_sent = False
                    if rabbitmq_enabled:
                        try:
                            rabbitmq_hook = RabbitMQHook(rabbitmq_conn_id=rabbitmq_conn_id)
                            message = {
                                "file": file_url,
                                "filename": filename,
                                "size_bytes": file_size,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "params": data_params
                            }
                            rabbitmq_hook.publish(
                                exchange=rabbitmq_exchange,
                                routing_key=rabbitmq_routing_key,
                                message=message,
                                exchange_type=rabbitmq_exchange_type,
                                properties={
                                    'delivery_mode': rabbitmq_delivery_mode,
                                    'priority': rabbitmq_priority,
                                    'content_type': rabbitmq_content_type,
                                    'timestamp': int(datetime.now(timezone.utc).timestamp())
                                }
                            )
                            rabbitmq_hook.close()
                            rabbitmq_sent = True
                        except Exception as rmq_error:
                            print(f"Failed to send message to RabbitMQ: {str(rmq_error)}")

                    results.append({
                        "filename": filename,
                        "s3_key": s3_key,
                        "file_url": file_url,
                        "size_bytes": file_size,
                        "rabbitmq_sent": rabbitmq_sent,
                        "status": "success"
                    })

                    print(f"Successfully processed: {filename}")

                    if cleanup_temp_files:
                        os.remove(temp_path)
                        print(f"Cleaned up temporary file: {temp_path}")

                else:
                    results.append({
                        "filename": filename,
                        "status": "failed",
                        "error": "Download failed"
                    })

            except Exception as e:
                error_msg = f"Error processing {url}: {str(e)}"
                print(error_msg)
                results.append({
                    "url": url,
                    "filename": url.split("/")[-1] if "/" in url else url,
                    "status": "failed",
                    "error": str(e),
                    "rabbitmq_sent": False
                })
        return results

    # 5. AGGREGATE RESULTS
    @task(task_id="aggregate_results")
    def aggregate_results(results_list: List[List[Dict]], config: Dict) -> Dict:
        """Aggregates results from all workers."""
        rabbitmq_enabled = config.get("rabbitmq_enabled", True)

        all_results = []
        success_count = 0
        failed_count = 0
        total_size = 0
        rabbitmq_sent_count = 0

        for worker_results in results_list:
            for result in worker_results:
                all_results.append(result)
                if result.get("status") == "success":
                    success_count += 1
                    total_size += result.get("size_bytes", 0)
                    if result.get("rabbitmq_sent"):
                        rabbitmq_sent_count += 1
                else:
                    failed_count += 1

        summary = {
            "total_files": len(all_results),
            "successful": success_count,
            "failed": failed_count,
            "total_size_bytes": total_size,
            "rabbitmq_enabled": rabbitmq_enabled,
            "rabbitmq_sent_count": rabbitmq_sent_count,
            "execution_time": datetime.now(timezone.utc).isoformat(),
            "results": all_results
        }

        print(f"Summary: {success_count} successful, {failed_count} failed, "
              f"total size: {total_size / (1024**3):.2f} GB, "
              f"RabbitMQ messages sent: {rabbitmq_sent_count}")

        if failed_count > 0:
            failed_files = [r.get('filename', 'unknown') for r in all_results if r.get('status') == 'failed']
            print(f"Failed files: {failed_files}")

        return summary

    # 6. CLEANUP TASK (optional)
    @task(task_id="cleanup_temp_files", trigger_rule="all_done")
    def cleanup_temp_files(config: Dict):
        """Cleans up remaining temporary files."""
        cleanup_temp_files_config = config.get("cleanup_temp_files", True)
        if not cleanup_temp_files_config:
            print("Cleanup of temporary files is disabled in configuration")
            return

        import glob
        temp_files = glob.glob("/tmp/*.grib2")
        cleaned_count = 0

        for file in temp_files:
            try:
                os.remove(file)
                cleaned_count += 1
                print(f"Cleaned up: {file}")
            except Exception as e:
                print(f"Error cleaning up {file}: {e}")

        print(f"Total cleaned temporary files: {cleaned_count}")

    # 7. RABBITMQ CONNECTION TEST TASK (optional)
    @task(task_id="test_rabbitmq_connection", trigger_rule="all_done")
    def test_rabbitmq_connection(config: Dict):
        """Tests connection to RabbitMQ if enabled."""
        rabbitmq_enabled = config.get("rabbitmq_enabled", True)
        rabbitmq_conn_id = config.get("rabbitmq_conn_id", "rabbitmq_default")

        if not rabbitmq_enabled:
            print("RabbitMQ is disabled in configuration")
            return {"status": "disabled"}

        try:
            rabbitmq_hook = RabbitMQHook(rabbitmq_conn_id=rabbitmq_conn_id)
            connection = rabbitmq_hook.get_conn()
            print(f"Successfully connected to RabbitMQ: {connection}")
            connection.close()
            return {"status": "connected", "connection_id": rabbitmq_conn_id}
        except Exception as e:
            print(f"RabbitMQ connection test failed: {str(e)}")
            return {"status": "failed", "error": str(e)}

    # ===== EXECUTION FLOW DEFINITION =====
    # 1. Load configuration
    config = load_config_and_setup()

    # 2. Prepare endpoint for sensor and check service availability
    endpoint = prepare_sensor_endpoint()
    # Dependency: config >> endpoint >> check_ecmwf_availability
    endpoint.set_upstream(config)

    # 3. Generate download URLs (needs both config and availability check)
    url_data = generate_urls(config=config)
    url_data.set_upstream(check_ecmwf_availability)

    # 4. Parallel file download and processing
    download_results = download_and_process(
        urls_chunk=url_data["distributed_urls"],
        data_params=url_data["params"],
        config=config
    )

    # 5. Aggregate results, perform cleanup and connection test
    final_summary = aggregate_results(download_results, config=config)
    cleanup = cleanup_temp_files(config=config)
    rabbitmq_test = test_rabbitmq_connection(config=config)

    # Final dependencies
    final_summary.set_upstream(download_results)
    cleanup.set_upstream(final_summary)
    rabbitmq_test.set_upstream(final_summary)