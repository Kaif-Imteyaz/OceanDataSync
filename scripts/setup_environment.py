#!/usr/bin/env python3
"""
Setup environment for Ocean Data Pipeline
"""

import subprocess
import sys
from pathlib import Path

def main():
    print("Setting up Ocean Data Pipeline environment...")
    
    # Define base path
    base_path = Path.cwd() / "DataPipeline"
    
    # Create directory structure
    directories = [
        base_path / "data" / "raw",
        base_path / "data" / "processed" / "noaa",
        base_path / "data" / "processed" / "copernicus",
        base_path / "data" / "processed" / "argo",
        base_path / "data" / "processed" / "ncei",
        base_path / "logs",
        base_path / "config"
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"Created: {directory}")
    
    # Create sample settings file
    settings_content = """# Ocean Data Pipeline Configuration

data_sources:
  noaa:
    enabled: true
    days_back: 2
  copernicus:
    enabled: true
    days_back: 1
  argo:
    enabled: true
    profile_limit: 5
  ncei:
    enabled: true
    station_limit: 10

processing:
  chunk_size: 9998
  cleanup_temp_files: true

logging:
  level: INFO
  save_csv: true
  save_metadata: true
"""
    
    settings_file = base_path / "config" / "settings.yaml"
    settings_file.write_text(settings_content)
    print(f"Created: {settings_file}")
    
    # Create .env.example
    env_example = """# Ocean Data Pipeline Environment Variables
# Copy this file to .env and fill in your credentials

# Copernicus Marine credentials (optional for basic access)
# COPERNICUS_USERNAME=your_username
# COPERNICUS_PASSWORD=your_password

# NOAA region (California coast by default)
NOAA_LAT_MIN=32.0
NOAA_LAT_MAX=35.0
NOAA_LON_MIN=-120.0
NOAA_LON_MAX=-115.0

# Processing settings
MAX_ROWS_PER_FILE=9998
REQUEST_TIMEOUT=30
"""
    
    env_file = base_path / ".env.example"
    env_file.write_text(env_example)
    print(f"Created: {env_file}")
    
    # Create requirements.txt
    requirements = """requests>=2.31.0
pandas>=2.1.0
numpy>=1.24.0
xarray>=2023.12.0
netCDF4>=1.6.5
pyyaml>=6.0
copernicusmarine>=0.8.0
h5py>=3.10.0
scipy>=1.11.0
beautifulsoup4>=4.12.0
"""
    
    req_file = base_path / "requirements.txt"
    req_file.write_text(requirements)
    print(f"Created: {req_file}")
    
    print("\nSetup complete!")
    print(f"Project directory: {base_path}")
    print("\nNext steps:")
    print("1. Install dependencies: pip install -r requirements.txt")
    print("2. Copy .env.example to .env and configure if needed")
    print("3. Run the pipeline: python scripts/run_pipeline.py")

if __name__ == "__main__":
    main()