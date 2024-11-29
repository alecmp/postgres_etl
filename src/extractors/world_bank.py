from typing import Dict, Any, List
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from src.extractors.base import BaseExtractor

class WorldBankExtractor(BaseExtractor):
    """Extract economic indicators data from World Bank API."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://api.worldbank.org/v2"
        self.indicators = config['world_bank_params']['indicators']
    
    def extract(self, **kwargs) -> Path:
        """
        Extract data from World Bank API.
        
        Args:
            **kwargs: Additional parameters including:
                - start_year: Start year for data extraction
                - end_year: End year for data extraction
                - countries: List of country codes
        """
        self.logger.info("Starting World Bank data extraction")
        
        all_data = []
        countries = kwargs.get('countries', ['all'])
        start_year = kwargs.get('start_year', '2000')
        end_year = kwargs.get('end_year', str(datetime.now().year))

        try:
            for indicator in self.indicators:
                self.logger.info(f"Fetching indicator: {indicator}")
                
                for country in countries:
                    url = f"{self.base_url}/countries/{country}/indicators/{indicator}"
                    params = {
                        'format': 'json',
                        'per_page': 1000,
                        'date': f"{start_year}:{end_year}",
                    }
                    
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    
                    # World Bank API returns a list where [0] is metadata and [1] is data
                    data = response.json()[1]
                    
                    # Transform to flat structure
                    for entry in data:
                        all_data.append({
                            'country': entry['country']['id'],
                            'country_name': entry['country']['value'],
                            'indicator': indicator,
                            'indicator_name': entry['indicator']['value'],
                            'value': float(entry['value']) if entry['value'] is not None else None,
                            'date': entry['date'],
                            'source': 'World Bank'
                        })
                        
            # Create DataFrame and save to bronze layer
            df = pd.DataFrame(all_data)
            filepath = self._save_bronze_data(df, 'world_bank')
            
            self.logger.info(f"World Bank data extraction completed: {filepath}")
            return filepath
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching World Bank data: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing World Bank data: {str(e)}")
            raise