import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.etl.WorldBankExtractor import WorldBankExtractor
from pprint import pprint
import logging

logging.basicConfig(level=logging.INFO)

def main():
    try:
        # Initialize the extractor
        extractor = WorldBankExtractor()
        
        # Extract data
        result = extractor.extract_data(
            country="IT",
            indicator="NY.GDP.MKTP.CD",  # GDP (current US$)
            start_year=2020,
            end_year=2022
        )
        
        # Print extraction metrics
        print("\n=== Extraction Metrics ===")
        metrics = result['metrics']
        print(f"Start time: {metrics['start_time']}")
        print(f"End time: {metrics['end_time']}")
        print(f"Records extracted: {metrics['records_extracted']}")
        print(f"Failed attempts: {metrics['failed_attempts']}")
        
        # Print metadata
        print("\n=== Metadata ===")
        metadata = result['metadata']
        print(f"Country: {metadata['country']}")
        print(f"Indicator: {metadata['indicator']}")
        print(f"Time Range: {metadata['time_range']}")
        print(f"Extraction Timestamp: {metadata['extraction_timestamp']}")
        
        # Print the actual data
        print("\n=== Extracted Data ===")
        data = result['extracted_data']
        for record in data:
            print(f"Year: {record['date']}, GDP: ${record['value']:,.2f}")

    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

if __name__ == "__main__":
    main()