"""
ECMWF Utilities Module
Contains helper functions for ECMWF data processing
"""
from datetime import datetime, timedelta
from typing import Dict, List
from airflow.exceptions import AirflowException


def _floor_hours(hour: int) -> int:
    """
    Rounds the hour from 0-24 format to the nearest floor values from [0, 6, 12, 18] array.
    
    It is more preferable than math round because ECMWF might not have the necessary data at the time.

    Example:
        ```
        hour1 = _round_hours(14) # hour1 == 12
        hour2 = _round_hours(17) # hour2 == 12
        hour3 = _round_hours(18) # hour3 == 18
        ```
    
    Params:
        hour (int): Current hour that needs to be floored
    Returns:
        (int): floors hour to nearest value from [0, 6, 12, 18] array
    """
    # 'if's might be replaced with match/case notation, 
    # but it is not supported in Python version 3.7 or older, 
    # so we use basic 'if' just in case
    if hour < 6:
        return 0
    if hour < 12:
        return 6
    if hour < 18:
        return 12
    return 18

def calculate_ecmwf_params(data_interval_start: datetime) -> Dict[str, str]:
    """
    Calculate parameters for ECMWF URL based on DAG execution date.
    Params:
        data_interval_start (datetime): date and time of DAG execution
    Returns:
        Dict: dictionary with ECMWF Open Data keys: date_str, time_str, data_type, base_url
    """
    # Date from previous day
    target_date = data_interval_start - timedelta(days=1)
    date_str = target_date.strftime("%Y%m%d")
    
    # DAG execution time converts from basic datetime to value in ["00z", "06z", "12z", "18z"]
    hour = _floor_hours(data_interval_start.hour)
    time_str = f"{hour:02d}z"
    
    # Determine data type
    data_type = "oper" if hour in [0, 12] else "scda"
    
    # Base URL for availability check and downloading files from that directory
    base_url = f"https://data.ecmwf.int/forecasts/{date_str}/{time_str}/"
    # base_url = f"https://data.ecmwf.int/forecasts/20260113/12z/"
    
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
        url = f"{params['base_url']}ifs/0p25/{params['data_type']}/{filename}"
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