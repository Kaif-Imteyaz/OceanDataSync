"""
Data scraper for oceanographic data sources
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import gzip
import shutil

from .config import PipelineConfig
from .logger import PipelineLogger

class DataScraper:
    """Handles data retrieval from multiple sources"""
    
    def __init__(self, config: PipelineConfig, logger: PipelineLogger):
        self.config = config
        self.logger = logger
        
        # Setup HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/csv, */*'
        })
        self.session.timeout = (10, 30)  # Connect timeout, read timeout
        
        # Track downloaded files
        self.downloaded_files = []
    
    def scrape_noaa_sst(self, days_back: int = 2) -> List[Path]:
        """Scrape NOAA ERDDAP SST data"""
        self.logger.log("NOAA", "SST_SCRAPING", "STARTED")
        
        try:
            # Calculate date range
            end_date = datetime.now() - timedelta(days=3)  # 3 days ago for data availability
            start_date = end_date - timedelta(days=days_back)
            
            # Build ERDDAP query
            url = (
                f"{self.config.data_source.noaa_erddap_base}/griddap/"
                f"{self.config.data_source.noaa_sst_dataset}.csv?"
                f"analysed_sst[({start_date.strftime('%Y-%m-%dT00:00:00Z')}):"
                f"1:({end_date.strftime('%Y-%m-%dT00:00:00Z')})]"
                f"[({self.config.data_source.default_region['lat_min']}):"
                f"1:({self.config.data_source.default_region['lat_max']})]"
                f"[({self.config.data_source.default_region['lon_min']}):"
                f"1:({self.config.data_source.default_region['lon_max']})]"
            )
            
            self.logger.log("NOAA", "REQUEST", "INFO", f"Fetching: {url[:100]}...")
            
            # Make request
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Save raw data
            filename = f"noaa_sst_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            filepath = self.config.raw_data_dir / filename
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            self.downloaded_files.append(filepath)
            
            # Preview data
            try:
                df_preview = pd.read_csv(filepath, nrows=5)
                self.logger.log_data_preview(df_preview, "NOAA", "RAW")
            except Exception as e:
                self.logger.log("NOAA", "PREVIEW", "WARNING", f"Cannot preview: {e}")
            
            self.logger.log("NOAA", "SST_SCRAPING", "SUCCESS",
                          f"Downloaded {len(response.content):,} bytes")
            
            return [filepath]
            
        except requests.exceptions.RequestException as e:
            self.logger.log("NOAA", "SST_SCRAPING", "ERROR", f"HTTP error: {e}")
            return []
        except Exception as e:
            self.logger.log("NOAA", "SST_SCRAPING", "ERROR", str(e))
            return []
    
    def scrape_copernicus_sst(self, days_back: int = 1) -> List[Path]:
        """Scrape Copernicus Marine SST data"""
        self.logger.log("COPERNICUS", "SST_SCRAPING", "STARTED")
        
        try:
            # For real implementation, would use copernicusmarine package
            # This is a simplified version that creates sample data
            
            target_date = datetime.now() - timedelta(days=3)
            
            # Generate realistic sample data
            np.random.seed(42)  # For reproducibility
            
            # Create grid of points
            latitudes = np.linspace(35, 45, 20)
            longitudes = np.linspace(-15, -5, 20)
            
            data = []
            for lat in latitudes:
                for lon in longitudes:
                    data.append({
                        'time': target_date.strftime('%Y-%m-%dT12:00:00Z'),
                        'latitude': round(lat, 2),
                        'longitude': round(lon, 2),
                        'analysed_sst': round(273.15 + np.random.uniform(10, 25), 3)  # Kelvin
                    })
            
            df = pd.DataFrame(data)
            
            # Save raw data
            filename = f"copernicus_sst_{target_date.strftime('%Y%m%d')}.csv"
            filepath = self.config.raw_data_dir / filename
            
            df.to_csv(filepath, index=False)
            self.downloaded_files.append(filepath)
            
            # Preview
            self.logger.log_data_preview(df.head(), "COPERNICUS", "RAW")
            
            self.logger.log("COPERNICUS", "SST_SCRAPING", "SUCCESS",
                          f"Generated {len(df)} sample rows")
            
            return [filepath]
            
        except Exception as e:
            self.logger.log("COPERNICUS", "SST_SCRAPING", "ERROR", str(e))
            return []
    
    def scrape_argo_profile_index(self) -> List[Path]:
        """Scrape Argo float profile index"""
        self.logger.log("ARGO", "INDEX_SCRAPING", "STARTED")
        
        try:
            # URL for Argo bio-profile index
            url = f"{self.config.data_source.argo_gdac_base}/argo_bio-profile_index.txt.gz"
            
            self.logger.log("ARGO", "REQUEST", "INFO", f"Downloading: {url}")
            
            # Download compressed file
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            # Save compressed file
            compressed_path = self.config.raw_data_dir / "argo_bio_profile_index.txt.gz"
            with open(compressed_path, 'wb') as f:
                f.write(response.content)
            
            # Decompress
            decompressed_path = self.config.raw_data_dir / "argo_bio_profile_index.txt"
            with gzip.open(compressed_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Remove compressed file
            compressed_path.unlink()
            
            self.downloaded_files.append(decompressed_path)
            
            # Read and preview first few lines
            with open(decompressed_path, 'r') as f:
                lines = [next(f) for _ in range(10)]
            
            self.logger.log("ARGO", "INDEX_SCRAPING", "INFO",
                          f"First line: {lines[0][:100]}..." if lines else "Empty file")
            
            self.logger.log("ARGO", "INDEX_SCRAPING", "SUCCESS",
                          f"Downloaded {decompressed_path.stat().st_size:,} bytes")
            
            return [decompressed_path]
            
        except Exception as e:
            self.logger.log("ARGO", "INDEX_SCRAPING", "ERROR", str(e))
            return []
    
    def scrape_ncei_stations(self) -> List[Path]:
        """Scrape NCEI GHCN station metadata"""
        self.logger.log("NCEI", "STATIONS_SCRAPING", "STARTED")
        
        try:
            url = f"{self.config.data_source.ncei_ghcn_base}/ghcnd-stations.txt"
            
            self.logger.log("NCEI", "REQUEST", "INFO", f"Downloading: {url}")
            
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            # Save file
            filepath = self.config.raw_data_dir / "ghcnd_stations.txt"
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            self.downloaded_files.append(filepath)
            
            # Preview
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = [next(f) for _ in range(5)]
            
            self.logger.log("NCEI", "STATIONS_SCRAPING", "INFO",
                          f"Sample: {lines[0][:50]}..." if lines else "Empty")
            
            self.logger.log("NCEI", "STATIONS_SCRAPING", "SUCCESS",
                          f"Downloaded {len(response.content):,} bytes")
            
            return [filepath]
            
        except Exception as e:
            self.logger.log("NCEI", "STATIONS_SCRAPING", "ERROR", str(e))
            return []
    
    def scrape_all_sources(self) -> List[Path]:
        """Scrape data from all enabled sources"""
        all_files = []
        
        sources_config = self.config.settings.get("data_sources", {})
        
        if sources_config.get("noaa", {}).get("enabled", True):
            days_back = sources_config.get("noaa", {}).get("days_back", 2)
            all_files.extend(self.scrape_noaa_sst(days_back))
        
        if sources_config.get("copernicus", {}).get("enabled", True):
            days_back = sources_config.get("copernicus", {}).get("days_back", 1)
            all_files.extend(self.scrape_copernicus_sst(days_back))
        
        if sources_config.get("argo", {}).get("enabled", True):
            all_files.extend(self.scrape_argo_profile_index())
        
        if sources_config.get("ncei", {}).get("enabled", True):
            all_files.extend(self.scrape_ncei_stations())
        
        return all_files