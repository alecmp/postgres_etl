from typing import Dict, Any, List
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from src.extractors.base import BaseExtractor

class IMFExtractor(BaseExtractor):
    """Extract economic indicators data from IMF API."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc"
        self.datasets = config['imf_params']['datasets']
    
    def extract(self, **kwargs) -> Path:
        """
        Extract data from IMF API.
        
        Args:
            **kwargs: Additional parameters including:
                - start_period: Start period for data extraction
                - end_period: End period for data extraction
                - countries: List of country codes
        """
        self.logger.info("Starting IMF data extraction")
        
        all_data = []
        countries = kwargs.get('countries', [])
        start_period = kwargs.get('start_period', '2000')
        end_period = kwargs.get('end_period', str(datetime.now().year))

        try:
            for dataset in self.datasets:
                self.logger.info(f"Fetching dataset: {dataset}")
                
                # First, get the data structure definition
                url = f"{self.base_url}/DataStructure/{dataset}"
                response = requests.get(url)
                response.raise_for_status()
                
                # Then fetch the actual data
                data_url = f"{self.base_url}/CompactData/{dataset}"
                params = {
                    'startPeriod': start_period,
                    'endPeriod': end_period
                }
                
                if countries:
                    params['references'] = 'all'
                    params['countries'] = '+'.join(countries)
                
                response = requests.get(data_url, params=params)
                response.raise_for_status()
                
                # Parse IMF's SDMX-JSON format
                data = response.json()
                series = data['CompactData']['DataSet']['Series']
                
                # Handle both single series and multiple series cases
                if not isinstance(series, list):
                    series = [series]
                
                for serie in series:
                    base_attributes = {
                        'country': serie.get('@REF_AREA', ''),
                        'indicator': serie.get('@INDICATOR', ''),
                        'frequency': serie.get('@FREQ', ''),
                        'source': 'IMF'
                    }
                    
                    # Handle observations
                    observations = serie.get('Obs', [])
                    if not isinstance(observations, list):
                        observations = [observations]
                    
                    for obs in observations:
                        entry = base_attributes.copy()
                        entry.update({
                            'date': obs.get('@TIME_PERIOD', ''),
                            'value': float(obs.get('@OBS_VALUE', 0)) if obs.get('@OBS_VALUE') is not None else None,
                            'status': obs.get('@STATUS', '')
                        })
                        all_data.append(entry)
            
            # Create DataFrame and save to bronze layer
            df = pd.DataFrame(all_data)
            filepath = self._save_bronze_data(df, 'imf')
            
            self.logger.info(f"IMF data extraction completed: {filepath}")
            return filepath
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching IMF data: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing IMF data: {str(e)}")
            raise