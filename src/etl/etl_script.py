import logging
from pathlib import Path
from typing import Dict, Any
from src.etl.WorldBankExtractor import WorldBankExtractor
from src.etl.WorldBankTransformer import WorldBankTransformer, TransformerConfig
from src.etl.WorldBankLoader import WorldBankLoader, LoaderConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_etl_pipeline(
    country: str,
    indicator: str,
    start_year: int,
    end_year: int,
    db_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Run the complete ETL pipeline.
    
    Args:
        country: Country code
        indicator: World Bank indicator code
        start_year: Start year for data
        end_year: End year for data
        db_config: Database configuration dictionary
        
    Returns:
        Dictionary containing pipeline execution statistics
    """
    try:
        # Extract
        extractor = WorldBankExtractor()
        extract_result = extractor.extract_data(
            country=country,
            indicator=indicator,
            start_year=start_year,
            end_year=end_year
        )
        
        # Transform
        transformer = WorldBankTransformer(
            config=TransformerConfig(
                drop_null_values=True,
                date_format="%Y",
                value_precision=2
            )
        )
        transform_result = transformer.transform_data(
            input_file=extract_result['metadata']['output_file']
        )
        
        # Load
        loader = WorldBankLoader(
            config=LoaderConfig(
                db_url=db_config['db_url'],
                schema_name=db_config['schema_name'],
                batch_size=db_config['batch_size']
            )
        )
        load_result = loader.load_data(
            input_file=transform_result['metadata']['output_file']
        )
        
        # Compile pipeline results
        pipeline_result = {
            'extract_metrics': extract_result['metrics'],
            'transform_metrics': transform_result['metadata'],
            'load_metrics': load_result['metadata'],
            'pipeline_status': 'success'
        }
        
        logger.info("ETL pipeline completed successfully")
        return pipeline_result
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    db_config = {
        'db_url': 'postgresql://user:password@localhost:5432/worldbank',
        'schema_name': 'world_bank',
        'batch_size': 1000
    }
    
    pipeline_result = run_etl_pipeline(
        country='USA',
        indicator='NY.GDP.MKTP.CD',  # GDP indicator
        start_year=2000,
        end_year=2023,
        db_config=db_config
    )