import pytest
from src.etl.WorldBankExtractor import fetch_world_bank_data, get_config_path

def test_world_bank_extraction():
    """Test successful data extraction from World Bank API."""
    # Test parameters
    country = "IT"
    indicator = "NY.GDP.MKTP.CD"
    start_year = 2020
    end_year = 2022
    
    # Get the correct config path
    config_file = get_config_path()
    
    # Test the function
    data = fetch_world_bank_data(country, indicator, start_year, end_year, config_file)
    
    # Assert the response is valid
    assert data is not None
    assert isinstance(data, list)

def test_invalid_config_path():
    """Test handling of non-existent config file."""
    with pytest.raises(FileNotFoundError):
        fetch_world_bank_data("IT", "NY.GDP.MKTP.CD", 2020, 2022, "nonexistent.yaml")

def test_invalid_data_structure():
    """Test handling of invalid API response structure."""
    # This would need mocking the API response
    pass  # We can implement this later with mocking if needed