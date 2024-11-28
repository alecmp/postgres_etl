import pandas as pd
import logging
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
class TransformerConfig:
    """Data class for transformer configuration."""
    drop_null_values: bool = True
    date_format: str = "%Y"
    value_precision: int = 2

class DataTransformationError(Exception):
    """Raised when data transformation fails."""
    pass

class WorldBankTransformer:
    """
    Handles transformation of World Bank data.
    
    Features:
    - Data cleaning and standardization
    - Type conversion
    - Null handling
    - Data validation
    """

    def __init__(self, config: Optional[TransformerConfig] = None):
        """Initialize the transformer with configuration."""
        self.config = config or TransformerConfig()
        self._ensure_transformed_data_dir()

    def _ensure_transformed_data_dir(self) -> Path:
        """Ensure the transformed data directory exists."""
        transformed_data_dir = Path(__file__).parent.parent / "data" / "transformed"
        transformed_data_dir.mkdir(parents=True, exist_ok=True)
        return transformed_data_dir

    def transform_data(self, input_file: Union[str, Path]) -> Dict[str, Any]:
        """
        Transform World Bank data from raw format to structured format.
        
        Args:
            input_file: Path to the raw data file
            
        Returns:
            Dictionary containing transformed data and metadata
        """
        try:
            logger.info(f"Starting data transformation for {input_file}")
            
            # Read raw data
            with open(input_file, 'r') as f:
                raw_data = pd.read_json(f)
                
            # Extract the actual data from the nested structure
            df = pd.DataFrame(raw_data['extracted_data'].iloc[0])
            
            # Clean and transform data
            transformed_df = self._transform_dataframe(df)
            
            # Save transformed data
            transformed_data_dir = self._ensure_transformed_data_dir()
            output_filename = f"transformed_{Path(input_file).stem}.parquet"
            output_path = transformed_data_dir / output_filename
            
            transformed_df.to_parquet(output_path, index=False)
            
            # Prepare result
            result = {
                'transformed_data': transformed_df.to_dict(orient='records'),
                'metadata': {
                    'input_file': str(input_file),
                    'output_file': str(output_path),
                    'transformation_timestamp': datetime.now().isoformat(),
                    'rows_processed': len(transformed_df),
                    'columns': list(transformed_df.columns)
                }
            }
            
            logger.info(f"Successfully transformed {len(transformed_df)} records")
            return result
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise DataTransformationError(f"Failed to transform data: {str(e)}")

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation rules to the dataframe."""
        # Convert date strings to datetime
        df['date'] = pd.to_datetime(df['date'], format=self.config.date_format)
        
        # Convert values to numeric, handling any non-numeric values
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Round values to specified precision
        df['value'] = df['value'].round(self.config.value_precision)
        
        # Drop null values if configured
        if self.config.drop_null_values:
            df = df.dropna()
        
        # Sort by date
        df = df.sort_values('date')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df