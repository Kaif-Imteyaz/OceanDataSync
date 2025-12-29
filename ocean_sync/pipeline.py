"""
Main pipeline orchestration
"""

import time
from typing import Dict, List
from pathlib import Path
import sys

from .config import PipelineConfig
from .logger import PipelineLogger
from .scraper import DataScraper
from .processor import DataProcessor

class OceanDataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, base_path: str = None):
        # Initialize configuration
        self.config = PipelineConfig(base_path)
        
        # Initialize logger
        self.logger = PipelineLogger(self.config.logs_dir, 
                                   self.config.settings.get("logging", {}).get("level", "INFO"))
        
        # Initialize components
        self.scraper = DataScraper(self.config, self.logger)
        self.processor = DataProcessor(self.config, self.logger)
        
        # Track execution metrics
        self.metrics = {
            "start_time": None,
            "end_time": None,
            "raw_files": 0,
            "processed_files": 0,
            "errors": 0
        }
    
    def run(self, sources: List[str] = None):
        """Run the complete pipeline"""
        self.metrics["start_time"] = time.time()
        
        try:
            # Phase 1: Data Collection
            self._print_section("PHASE 1: DATA COLLECTION")
            
            if sources:
                # Collect specific sources
                raw_files = []
                for source in sources:
                    if source.lower() == "noaa":
                        raw_files.extend(self.scraper.scrape_noaa_sst())
                    elif source.lower() == "copernicus":
                        raw_files.extend(self.scraper.scrape_copernicus_sst())
                    elif source.lower() == "argo":
                        raw_files.extend(self.scraper.scrape_argo_profile_index())
                    elif source.lower() == "ncei":
                        raw_files.extend(self.scraper.scrape_ncei_stations())
            else:
                # Collect all enabled sources
                raw_files = self.scraper.scrape_all_sources()
            
            self.metrics["raw_files"] = len(raw_files)
            
            if not raw_files:
                self.logger.log("PIPELINE", "DATA_COLLECTION", "ERROR", "No data collected")
                return False
            
            # Phase 2: Data Processing
            self._print_section("PHASE 2: DATA PROCESSING")
            
            processed_by_source = self.processor.process_all(raw_files)
            
            # Count total processed files
            total_processed = 0
            for source_files in processed_by_source.values():
                total_processed += len(source_files)
            
            self.metrics["processed_files"] = total_processed
            
            # Phase 3: Generate Report
            self._print_section("EXECUTION SUMMARY")
            self._generate_report(processed_by_source)
            
            return True
            
        except Exception as e:
            self.logger.log("PIPELINE", "EXECUTION", "ERROR", str(e))
            self.metrics["errors"] += 1
            return False
            
        finally:
            self.metrics["end_time"] = time.time()
            self.logger.save_logs()
    
    def _print_section(self, title: str):
        """Print section header"""
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
    
    def _generate_report(self, processed_by_source: Dict[str, List[Path]]):
        """Generate execution report"""
        execution_time = self.metrics["end_time"] - self.metrics["start_time"]
        
        print(f"\nExecution Time: {execution_time:.2f} seconds")
        print(f"Raw Files Collected: {self.metrics['raw_files']}")
        print(f"Processed Files Created: {self.metrics['processed_files']}")
        print(f"Max Rows per File: {self.config.max_rows_per_file:,}")
        print("\nBreakdown by Source:")
        
        for source, files in processed_by_source.items():
            if files:
                print(f"  {source.upper()}: {len(files)} file(s)")
        
        print(f"\nProject Directory: {self.config.base_path}")
        print(f"Raw Data: {self.config.raw_data_dir}")
        print(f"Processed Data: {self.config.processed_data_dir}")
        print(f"Logs: {self.config.logs_dir}")
        print("\n" + "=" * 70)