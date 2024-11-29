import os
import yaml
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class IMFExtractorConfig:
    """Data class for IMF extractor configuration."""
    base_url: str = "http://dataservices.imf.org/REST/SDMX_JSON.svc"
    retry_attempts: int = 3
    retry_backoff_factor: float = 0.3
    timeout: int = 30

class IMFExtractor:
    """
    Handles extraction of data from IMF API.
    
    Features:
    - Automatic retry with exponential backoff
    - Response validation
    - Error handling
    - Metric collection
    """

    def __init__(self, config: Optional[IMFExtractorConfig] = None):
        """Initialize the extractor with configuration."""
        self.config = config or IMFExtractorConfig()
        self.session = self._setup_session()
        self._ensure_raw_data_dir()

    def _setup_session(self) -> requests.Session:
        """Configure requests session with retry mechanism."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _ensure_raw_data_dir(self) -> Path:
        """Ensure the raw data directory exists."""
        raw_data_dir = Path(__file__).parent.parent / "data" / "raw" / "imf"
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        return raw_data_dir

    def extract_fsi_data(
        self, 
        country_code: str,
        start_year: int,
        end_year: int,
        indicators: List[str]
    ) -> Dict[str, Any]:
        """
        Extract Financial Soundness Indicators (FSI) from IMF.
        
        Args:
            country_code: ISO country code
            start_year: Start year for data extraction
            end_year: End year for data extraction
            indicators: List of FSI indicator codes
            
        Returns:
            Dictionary containing extracted data and metadata
        """
        try:
            logger.info(f"Extracting FSI data for {country_code}")
            
            # Construct the IMF API URL for FSI data
            url = f"{self.config.base_url}/CompactData/FSI/{country_code}"
            
            # Add query parameters
            params = {
                "startPeriod": f"{start_year}",
                "endPeriod": f"{end_year}",
                "indicators": ",".join(indicators)
            }
            
            # Make the request
            response = self.session.get(url, params=params, timeout=self.config.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract the actual data series from the IMF's nested structure
            series_data = self._parse_imf_response(data)
            
            # Save raw data
            raw_data_dir = self._ensure_raw_data_dir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fsi_{country_code}_{timestamp}.json"
            filepath = raw_data_dir / filename
            
            result = {
                'extracted_data': series_data,
                'metadata': {
                    'country': country_code,
                    'indicators': indicators,
                    'time_range': f"{start_year}-{end_year}",
                    'extraction_timestamp': datetime.now().isoformat(),
                    'output_file': str(filepath)
                }
            }
            
            with open(filepath, 'w') as f:
                json.dump(result, f, indent=2)
            
            logger.info(f"Successfully extracted FSI data for {country_code}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract FSI data: {str(e)}")
            raise

    def _parse_imf_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse the nested IMF JSON response into a flat structure."""
        series_data = []
        
        try:
            # Navigate through IMF's complex JSON structure
            datasets = response_data['CompactData']['DataSet']
            series = datasets['Series']
            
            # Handle both single series and multiple series cases
            if not isinstance(series, list):
                series = [series]
            
            for s in series:
                indicator = s['@INDICATOR']
                
                # Handle both single observation and multiple observations
                obs = s['Obs']
                if not isinstance(obs, list):
                    obs = [obs]
                
                for ob in obs:
                    series_data.append({
                        'indicator': indicator,
                        'date': ob['@TIME_PERIOD'],
                        'value': float(ob['@OBS_VALUE'])
                    })
        
        except KeyError as e:
            logger.error(f"Failed to parse IMF response: {str(e)}")
            raise
            
        return series_data