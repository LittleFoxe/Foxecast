"""
ECMWF Downloader Module
Contains classes and functions for downloading data from ECMWF
"""
import requests
from airflow.exceptions import AirflowException


class ECMWFDownloader:
    """
    Class for downloading files from ECMWF with retry support
    """
    
    def __init__(self, max_retries: int = 3, retry_delay: int = 30):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        
        # Settings to avoid HTTP 429
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; ECMWF-Downloader/1.0)'
        })
    
    def download_file(self, url: str, local_path: str) -> bool:
        """
        Download file with retries
        """
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, stream=True, timeout=60)
                
                if response.status_code == 200:
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    return True
                elif response.status_code == 429:
                    # Too many requests - wait longer
                    import time
                    wait_time = self.retry_delay * (attempt + 2)
                    time.sleep(wait_time)
                    continue
                else:
                    raise AirflowException(f"HTTP {response.status_code} for {url}")
                    
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise AirflowException(f"Error downloading {url}: {str(e)}")
                import time
                time.sleep(self.retry_delay)
        
        return False