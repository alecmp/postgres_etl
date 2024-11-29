from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import logging
from pathlib import Path

class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_bronze_storage()
    
    def _setup_bronze_storage(self) -> None:
        """Set up bronze (raw) data storage."""
        self.bronze_path = Path(self.config['data_paths']['bronze'])
        self.bronze_path.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def extract(self, **kwargs) -> Dict[str, Any]:
        """Extract data from source."""
        pass
    
    def _save_bronze_data(self, data: Dict[str, Any], source: str) -> Path:
        """Save raw data to bronze layer."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = self.bronze_path / f"{source}_{timestamp}.parquet"
        pd.DataFrame(data).to_parquet(filepath, index=False)
        return filepath