import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
from pathlib import Path
from typing import Dict, Any

from world_bank_extractor import WorldBankExtractor
from world_bank_transformer import WorldBankTransformer, TransformerConfig
from world_bank_loader import WorldBankLoader, LoaderConfig
from imf_extractor import IMFExtractor

def run_combined_etl(
    country: str,
    start_year: int,
    end_year: int,
    db_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Run ETL pipeline for both World Bank and IMF data."""
    
    # World Bank indicators
    wb_indicators = {
        'NY.GDP.MKTP.KD.ZG': 'GDP growth (annual %)',
        'SL.UEM.TOTL.ZS': 'Unemployment, total (% of labor force)'
    }
    
    # IMF Financial Soundness Indicators
    imf_indicators = [
        'FSANL_PT', # Bank Regulatory Capital to Risk-Weighted Assets
        'FSANL_NL', # Bank Nonperforming Loans to Total Loans
        'FSANL_CA'  # Bank Return on Assets
    ]
    
    results = {}
    
    # Extract and load World Bank data
    wb_extractor = WorldBankExtractor()
    wb_transformer = WorldBankTransformer()
    wb_loader = WorldBankLoader(LoaderConfig(**db_config))
    
    for indicator_code, indicator_name in wb_indicators.items():
        # Extract
        wb_data = wb_extractor.extract_data(
            country=country,
            indicator=indicator_code,
            start_year=start_year,
            end_year=end_year
        )
        
        # Transform
        transformed_wb = wb_transformer.transform_data(wb_data['metadata']['output_file'])
        
        # Load
        wb_loader.load_data(transformed_wb['metadata']['output_file'])
        
        results[indicator_name] = wb_data
    
    # Extract and load IMF data
    imf_extractor = IMFExtractor()
    imf_data = imf_extractor.extract_fsi_data(
        country_code=country,
        start_year=start_year,
        end_year=end_year,
        indicators=imf_indicators
    )
    
    # Transform IMF data
    transformed_imf = wb_transformer.transform_data(imf_data['metadata']['output_file'])
    
    # Load IMF data
    wb_loader.load_data(transformed_imf['metadata']['output_file'])
    
    results['IMF_FSI'] = imf_data
    
    return results

def create_analysis_visualization(db_config: Dict[str, Any], country: str):
    """Create visualization of the combined economic data."""
    engine = create_engine(db_config['db_url'])
    
    # Query combined data
    query = """
    SELECT 
        wb_gdp.date,
        wb_gdp.value as gdp_growth,
        wb_unemp.value as unemployment_rate,
        imf_fsi.regulatory_capital,
        imf_fsi.nonperforming_loans,
        imf_fsi.return_on_assets
    FROM 
        world_bank.economic_indicators wb_gdp
    JOIN 
        world_bank.economic_indicators wb_unemp 
        ON wb_gdp.date = wb_unemp.date
    JOIN 
        imf.financial_soundness imf_fsi 
        ON wb_gdp.date = imf_fsi.date
    WHERE 
        wb_gdp.indicator = 'NY.GDP.MKTP.KD.ZG'
        AND wb_unemp.indicator = 'SL.UEM.TOTL.ZS'
        AND wb_gdp.country = :country
    ORDER BY 
        wb_gdp.date
    """
    
    df = pd.read_sql(query, engine, params={'country': country})
    
    # Create visualization
    fig = px.line(df, x='date', y=['gdp_growth', 'unemployment_rate', 
                                  'regulatory_capital', 'nonperforming_loans', 
                                  'return_on_assets'],
                  title=f'Economic Indicators for {country}')
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Value",
        legend_title="Indicators"
    )
    
    return fig

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Database configuration
    db_config = {
        'db_url': os.getenv('DATABASE_URL'),
        'schema_name': 'economic_data',
        'batch_size': 1000
    }
    
    # Run pipeline for USA
    results = run_combined_etl(
        country='USA',
        start_year=2010,
        end_year=2023,
        db_config=db_config
    )
    
    # Create visualization
    fig = create_analysis_visualization(db_config, 'USA')
    fig.show()