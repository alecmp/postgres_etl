import pandas as pd
import logging
import sqlalchemy
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class LoaderConfig:
    """Data class for loader configuration."""
    db_url: str
    schema_name: str = "world_bank"
    batch_size: int = 1000
    if_exists: str = "append"

class DataLoadError(Exception):
    """Raised when data loading fails."""
    pass

class WorldBankLoader:
    """
    Handles loading of transformed World Bank data into a database.
    
    Features:
    - Database connection management
    - Batch processing
    - Error handling
    - Transaction management
    """

    def __init__(self, config: LoaderConfig):
        """Initialize the loader with configuration."""
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self) -> sqlalchemy.engine.Engine:
        """Create database engine."""
        return sqlalchemy.create_engine(self.config.db_url)

    def load_data(self, input_file: Union[str, Path]) -> Dict[str, Any]:
        """
        Load transformed data into the database.
        
        Args:
            input_file: Path to the transformed data file
            
        Returns:
            Dictionary containing load statistics and metadata
        """
        try:
            logger.info(f"Starting data load for {input_file}")
            
            # Read transformed data
            df = pd.read_parquet(input_file)
            
            # Create schema if it doesn't exist
            with self.engine.connect() as conn:
                conn.execute(sqlalchemy.text(
                    f"CREATE SCHEMA IF NOT EXISTS {self.config.schema_name}"
                ))
            
            # Load data in batches
            total_rows = 0
            for i in range(0, len(df), self.config.batch_size):
                batch = df.iloc[i:i + self.config.batch_size]
                
                batch.to_sql(
                    name='economic_indicators',
                    schema=self.config.schema_name,
                    con=self.engine,
                    if_exists=self.config.if_exists,
                    index=False,
                    method='multi'
                )
                
                total_rows += len(batch)
                logger.info(f"Loaded batch of {len(batch)} rows")
            
            result = {
                'metadata': {
                    'input_file': str(input_file),
                    'load_timestamp': datetime.now().isoformat(),
                    'rows_loaded': total_rows,
                    'target_schema': self.config.schema_name,
                    'target_table': 'economic_indicators'
                }
            }
            
            logger.info(f"Successfully loaded {total_rows} records")
            return result
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise DataLoadError(f"Failed to load data: {str(e)}")