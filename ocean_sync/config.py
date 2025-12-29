"""
Configuration management for Ocean Data Pipeline
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any
import yaml

@dataclass
class DataSourceConfig:
    """Configuration for data sources"""
    # NOAA ERDDAP
    noaa_erddap_base: str = "https://coastwatch.pfeg.noaa.gov/erddap"
    noaa_sst_dataset: str = "jplMURSST41"
    
    # Copernicus Marine
    copernicus_dataset_id: str = "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
    copernicus_product_id: str = "SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001"
    
    # Argo
    argo_gdac_base: str = "https://data-argo.ifremer.fr"
    
    # NCEI
    ncei_ghcn_base: str = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
    
    # Default regions
    default_region = {
        "lat_min": 32.0,
        "lat_max": 35.0,
        "lon_min": -120.0,
        "lon_max": -115.0
    }

class PipelineConfig:
    """Pipeline configuration manager"""
    
    def __init__(self, base_path: Optional[Path] = None):
        # Set base path
        if base_path:
            self.base_path = Path(base_path)
        else:
            # Default to current working directory
            self.base_path = Path.cwd() / "oceanDataPipeline"
        
        # Create directory structure
        self.data_dir = self.base_path / "data"
        self.raw_data_dir = self.data_dir / "raw"
        self.processed_data_dir = self.data_dir / "processed"
        self.logs_dir = self.base_path / "logs"
        self.config_dir = self.base_path / "config"
        
        # Create source-specific directories
        self.noaa_dir = self.processed_data_dir / "noaa"
        self.copernicus_dir = self.processed_data_dir / "copernicus"
        self.argo_dir = self.processed_data_dir / "argo"
        self.ncei_dir = self.processed_data_dir / "ncei"
        
        # Create all directories
        self._create_directories()
        
        # Load settings
        self.settings = self._load_settings()
        
        # Data source config
        self.data_source = DataSourceConfig()
        
        # Processing settings
        self.max_rows_per_file = 9000
        self.request_timeout = 30
        
    def _create_directories(self):
        """Create all necessary directories"""
        directories = [
            self.base_path,
            self.data_dir,
            self.raw_data_dir,
            self.processed_data_dir,
            self.logs_dir,
            self.config_dir,
            self.noaa_dir,
            self.copernicus_dir,
            self.argo_dir,
            self.ncei_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
        print(f"Project directory: {self.base_path}")
    
    def _load_settings(self) -> Dict[str, Any]:
        """Load settings from YAML file"""
        settings_file = self.config_dir / "settings.yaml"
        
        if settings_file.exists():
            with open(settings_file, 'r') as f:
                return yaml.safe_load(f)
        
        # Default settings
        default_settings = {
            "data_sources": {
                "noaa": {"enabled": True, "days_back": 2},
                "copernicus": {"enabled": True, "days_back": 1},
                "argo": {"enabled": True, "profile_limit": 5},
                "ncei": {"enabled": True, "station_limit": 10}
            },
            "processing": {
                "chunk_size": 9000,
                "cleanup_temp_files": True
            },
            "logging": {
                "level": "INFO",
                "save_csv": True,
                "save_metadata": True
            }
        }
        
        # Save default settings
        with open(settings_file, 'w') as f:
            yaml.dump(default_settings, f, default_flow_style=False)
        
        return default_settings
    
    def get_credentials(self, source: str) -> Dict[str, str]:
        """Get credentials for a data source"""
        # Check environment variables first
        username = os.environ.get(f"{source.upper()}_USERNAME")
        password = os.environ.get(f"{source.upper()}_PASSWORD")
        
        # Fallback to .env file
        if not username or not password:
            env_file = self.base_path / ".env"
            if env_file.exists():
                with open(env_file, 'r') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == f"{source.upper()}_USERNAME":
                                username = value
                            elif key == f"{source.upper()}_PASSWORD":
                                password = value
        
        return {"username": username, "password": password}