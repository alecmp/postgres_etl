"""
World Bank Data Extractor Module

This module handles the extraction phase of the ETL pipeline for World Bank data.
It follows industry best practices including:
- Separation of concerns
- Error handling and logging
- Data validation
- Retry mechanisms
- Configuration management
- Type hints and documentation
"""

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
class ExtractorConfig:
    """Data class for extractor configuration."""
    base_url: str
    default_format: str
    default_page_size: int
    retry_attempts: int
    retry_backoff_factor: float
    timeout: int

class DataValidationError(Exception):
    """Raised when data validation fails."""
    pass

class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass

class WorldBankExtractor:
    """
    Handles extraction of data from World Bank API.
    
    Features:
    - Automatic retry with exponential backoff
    - Response validation
    - Comprehensive error handling
    - Metric collection
    - Logging
    """

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """Initialize the extractor with configuration."""
        self.config = self._load_config(config_path)
        self.session = self._setup_session()
        self.extraction_metrics = {
            'start_time': None,
            'end_time': None,
            'records_extracted': 0,
            'failed_attempts': 0
        }

    def _load_config(self, config_path: Optional[Union[str, Path]] = None) -> ExtractorConfig:
        """Load and validate configuration."""
        try:
            if config_path is None:
                config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"

            with open(config_path, 'r') as file:
                config_data = yaml.safe_load(file)

            # Validate required configuration fields
            required_fields = ['base_url', 'default_format', 'default_page_size']
            if not all(field in config_data['world_bank'] for field in required_fields):
                raise ConfigurationError("Missing required configuration fields")

            return ExtractorConfig(
                base_url=config_data['world_bank']['base_url'],
                default_format=config_data['world_bank']['default_format'],
                default_page_size=config_data['world_bank']['default_page_size'],
                retry_attempts=config_data['world_bank'].get('retry_attempts', 3),
                retry_backoff_factor=config_data['world_bank'].get('retry_backoff_factor', 0.3),
                timeout=config_data['world_bank'].get('timeout', 10)
            )
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found at: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML configuration: {e}")

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

    def _validate_response_data(self, data: List[Any]) -> bool:
        """Validate the structure and content of the API response."""
        if not isinstance(data, list) or len(data) < 2:
            return False
        return all(isinstance(item, dict) and 'date' in item and 'value' in item 
                  for item in data[1])

    def _ensure_raw_data_dir(self) -> Path:
        """Ensure the raw data directory exists."""
        raw_data_dir = Path(__file__).parent.parent / "data" / "raw"
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        return raw_data_dir

    def extract_data(
        self, 
        country: str, 
        indicator: str, 
        start_year: int, 
        end_year: int
    ) -> Dict[str, Any]:
        """Extract data and save to disk."""
        self.extraction_metrics['start_time'] = datetime.now()
        
        try:
            url = f"{self.config.base_url}/country/{country}/indicator/{indicator}"
            params = {
                "format": self.config.default_format,
                "date": f"{start_year}:{end_year}",
                "per_page": self.config.default_page_size
            }

            logger.info(f"Starting data extraction for {country} - {indicator}")
            
            response = self.session.get(
                url, 
                params=params, 
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            if not self._validate_response_data(data):
                raise DataValidationError("Invalid data structure in API response")

            self.extraction_metrics['records_extracted'] = len(data[1])
            
            # Create result dictionary
            result = {
                'extracted_data': data[1],
                'metadata': {
                    'country': country,
                    'indicator': indicator,
                    'time_range': f"{start_year}-{end_year}",
                    'extraction_timestamp': datetime.now().isoformat()
                },
                'metrics': self.extraction_metrics
            }

            # Save raw data to disk
            raw_data_dir = self._ensure_raw_data_dir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{country}_{indicator}_{timestamp}.json"
            filepath = raw_data_dir / filename

            with open(filepath, 'w') as f:
                json.dump(result, f, indent=2)

            logger.info(f"Successfully extracted {len(data[1])} records and saved to {filepath}")
            return result

        except requests.exceptions.RequestException as e:
            self.extraction_metrics['failed_attempts'] += 1
            logger.error(f"API request failed: {str(e)}")
            raise

        except Exception as e:
            self.extraction_metrics['failed_attempts'] += 1
            logger.error(f"Extraction failed: {str(e)}")
            raise

        finally:
            self.extraction_metrics['end_time'] = datetime.now()