# src/pipeline/orchestrator.py
import logging
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

@dataclass
class PipelineMetrics:
    """Container for pipeline execution metrics."""
    start_time: datetime
    end_time: Optional[datetime] = None
    records_processed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    data_quality_scores: Dict[str, float] = field(default_factory=dict)

class PipelineOrchestrator:
    """
    Orchestrates the entire ETL pipeline with proper monitoring and error handling.
    
    Features:
    - Parallel processing where possible
    - Comprehensive logging and monitoring
    - Error handling and recovery
    - Data quality checks
    - Performance metrics collection
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.metrics = PipelineMetrics(start_time=datetime.now())
    
    def run_pipeline(self) -> Dict[str, Any]:
        """Execute the complete ETL pipeline."""
        try:
            self.logger.info("Starting ETL pipeline execution")
            
            # Extract data (parallel execution)
            bronze_paths = self._run_extraction()
            
            # Transform to silver (parallel execution)
            silver_paths = self._run_bronze_to_silver(bronze_paths)
            
            # Transform to gold (sequential due to data dependencies)
            gold_path = self._run_silver_to_gold(silver_paths)
            
            # Load to database
            self._run_database_load(gold_path)
            
            # Finalize metrics
            self.metrics.end_time = datetime.now()
            
            # Generate execution report
            report = self._generate_execution_report()
            
            self.logger.info("Pipeline execution completed successfully")
            return report
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            self.metrics.errors.append(str(e))
            raise
    
    def _run_extraction(self) -> List[Path]:
        """Run all extractors in parallel."""
        extractors = [
            (WorldBankExtractor(self.config), self.config['world_bank_params']),
            (IMFExtractor(self.config), self.config['imf_params'])
        ]
        
        with ThreadPoolExecutor() as executor:
         results = executor.map(self._extract_data, extractors)
    
        return [result for result in results if result is not None]

    
    def _extract_data(self, extractor: Tuple[Any, Dict[str, Any]]) -> Optional[Path]:
        """Extract data using the provided extractor."""
        extractor, params = extractor
        try:
            return extractor.extract(**params)
        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            return None