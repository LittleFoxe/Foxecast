"""
ECMWF Utilities Module
Contains helper functions for ECMWF data processing
"""
from datetime import datetime, timedelta
from typing import Dict, List
from airflow.exceptions import AirflowException


def calculate_ecmwf_params(execution_date: datetime) -> Dict[str, str]:
    """
    Calculate parameters for ECMWF URL based on DAG execution date.
    Params:
        execution_date (datetime): date and time of DAG execution
    Returns:
        Dict: dictionary with ECMWF Open Data keys: date_str, time_str, data_type, base_url
    """
    # Date from previous day
    target_date = execution_date - timedelta(days=1)
    date_str = target_date.strftime("%Y%m%d")
    
    # DAG execution time (00, 06, 12, 18) -> 00z, 06z, 12z, 18z
    hour = execution_date.hour
    time_str = f"{hour:02d}z"
    
    # Determine data type
    data_type = "oper" if hour in [0, 12] else "scda"
    
    # Base URL for availability check
    base_url = f"https://data.ecmwf.int/forecasts/{date_str}/{time_str}"
    
    return {
        "date_str": date_str,
        "time_str": time_str,
        "data_type": data_type,
        "base_url": base_url
    }


def generate_file_urls(params: Dict[str, str], steps: List[int]) -> List[str]:
    """
    Generate full URLs for all files
    """
    urls = []
    hour_str = params["time_str"].replace("z", "")  # 00z -> 00
    file_prefix = f"{params['date_str']}{hour_str}0000"
    
    for step in steps:
        filename = f"{file_prefix}-{step}h-{params['data_type']}-fc.grib2"
        url = f"{params['base_url']}/ifs/0p25/{params['data_type']}/{filename}"
        urls.append(url)
    
    return urls


def round_robin_distribute(urls: List[str], num_workers: int) -> List[List[str]]:
    """
    Distribute URLs among workers using round robin algorithm
    Params:
        urls (List[str]): list of files' URLs from ECMWF Open Data
        num_workers (int): number of tasks to distribute URLs to
    Returns:
        List[List[str]]: list of lists, each containing URLs for a worker
    """
    distributed = [[] for _ in range(num_workers)]
    
    for i, url in enumerate(urls):
        worker_idx = i % num_workers
        distributed[worker_idx].append(url)
    
    return distributed


def validate_ecmwf_response(response) -> bool:
    """
    Validate ECMWF API response
    """
    if response.status_code == 200:
        return True
    elif response.status_code == 429:
        # Rate limiting
        return False
    elif response.status_code == 404:
        raise AirflowException(f"ECMWF resource not found: {response.url}")
    else:
        raise AirflowException(f"Unexpected ECMWF response: {response.status_code}")