from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd
from datetime import datetime
from src.transformers.base import BaseTransformer
from src.utils.validation import DataValidator

class BronzeToSilverTransformer(BaseTransformer):
    """Transform raw data from bronze to silver layer with cleaning and standardization."""
    
    def transform(self, input_path: Path, **kwargs) -> Path:
        """
        Transform bronze (raw) data to silver (cleaned) format.
        
        Transformations include:
        - Data type standardization
        - Date normalization
        - Currency normalization
        - Null handling
        - Duplicate removal
        - Basic validation
        """
        self.logger.info(f"Starting bronze to silver transformation for {input_path}")
        
        # Read bronze data
        df = pd.read_parquet(input_path)
        
        # Apply transformations
        transformed_df = (df.pipe(self._standardize_datatypes)
                          .pipe(self._normalize_dates)
                          .pipe(self._handle_nulls)
                          .pipe(self._remove_duplicates)
                          .pipe(self._validate_data))
        
        # Save to silver layer
        source = input_path.stem.split('_')[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.silver_path / f"{source}_silver_{timestamp}.parquet"
        
        transformed_df.to_parquet(output_path, index=False)
        
        self.logger.info(f"Completed bronze to silver transformation: {output_path}")
        return output_path
    
    def _standardize_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types across columns."""
        type_mappings = {
            'date': 'datetime64[ns]',
            'value': 'float64',
            'indicator': 'string',
            'country': 'string'
        }
        
        for col, dtype in type_mappings.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        
        return df
    
    def _normalize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize dates to standard format."""
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].dt.year
            df['quarter'] = df['date'].dt.quarter
        return df
    
    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle null values according to business rules."""
        # Log null statistics
        null_stats = df.isnull().sum()
        self.logger.info(f"Null statistics before handling:\n{null_stats}")
        
        # Apply business rules for null handling
        rules = {
            'value': 'drop',  # Drop rows with null values
            'indicator': 'drop',  # Drop rows with null indicators
            'country': 'drop'  # Drop rows with null country codes
        }
        
        for col, action in rules.items():
            if col in df.columns:
                if action == 'drop':
                    df = df.dropna(subset=[col])
                    
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records."""
        before_count = len(df)
        df = df.drop_duplicates()
        after_count = len(df)
        
        if before_count != after_count:
            self.logger.warning(f"Removed {before_count - after_count} duplicate records")
        
        return df
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate transformed data."""
        validator = DataValidator()
        validation_rules = {
            'date': {'min_year': 1960, 'max_year': datetime.now().year},
            'value': {'min_value': -100, 'max_value': 1000000}
        }
        
        validation_results = validator.validate(df, validation_rules)
        
        if not validation_results['is_valid']:
            self.logger.error(f"Data validation failed: {validation_results['errors']}")
            raise ValueError("Data validation failed")
        
        return df