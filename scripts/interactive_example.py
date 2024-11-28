import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.etl.extract import WorldBankExtractor
import argparse
import logging

logging.basicConfig(level=logging.INFO)

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Extract World Bank Data')
    parser.add_argument('--country', default='IT', help='Country code (e.g., IT for Italy)')
    parser.add_argument('--indicator', default='NY.GDP.MKTP.CD', 
                       help='World Bank indicator code (e.g., NY.GDP.MKTP.CD for GDP)')
    parser.add_argument('--start-year', type=int, default=2020, help='Start year')
    parser.add_argument('--end-year', type=int, default=2022, help='End year')
    
    args = parser.parse_args()

    try:
        extractor = WorldBankExtractor()
        result = extractor.extract_data(
            country=args.country,
            indicator=args.indicator,
            start_year=args.start_year,
            end_year=args.end_year
        )
        
        # Print the results in a formatted way
        print("\n=== Extraction Results ===")
        print(f"Country: {args.country}")
        print(f"Indicator: {args.indicator}")
        print(f"Period: {args.start_year}-{args.end_year}")
        print("\n=== Data ===")
        
        for record in result['extracted_data']:
            print(f"Year: {record['date']}, Value: ${record['value']:,.2f}")
            
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()