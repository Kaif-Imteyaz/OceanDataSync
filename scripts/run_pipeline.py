#!/usr/bin/env python3
"""
Command-line interface for Ocean Data Pipeline
"""

import sys
import argparse
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ocean_data_pipeline.pipeline import OceanDataPipeline

def main():
    parser = argparse.ArgumentParser(description="Ocean Data Pipeline v7.0")
    
    parser.add_argument("--path", type=str, 
                       default="C:/Users/Kaif/Vscode/Projects/oceanDataPipeline",
                       help="Base path for the project")
    
    parser.add_argument("--sources", nargs="+",
                       choices=["noaa", "copernicus", "argo", "ncei"],
                       help="Specific data sources to process (default: all)")
    
    parser.add_argument("--install", action="store_true",
                       help="Install required dependencies")
    
    args = parser.parse_args()
    
    # Install dependencies if requested
    if args.install:
        install_dependencies()
    
    # Run pipeline
    try:
        print("Starting Ocean Data Pipeline...")
        
        pipeline = OceanDataPipeline(args.path)
        
        success = pipeline.run(args.sources)
        
        if success:
            print("\nPipeline completed successfully!")
            return 0
        else:
            print("\nPipeline completed with errors")
            return 1
            
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        return 130
    except Exception as e:
        print(f"\nFatal error: {e}")
        return 1

def install_dependencies():
    """Install required Python packages"""
    import subprocess
    import sys
    
    requirements = [
        "requests>=2.31.0",
        "pandas>=2.1.0",
        "numpy>=1.24.0",
        "xarray>=2023.12.0",
        "netCDF4>=1.6.5",
        "pyyaml>=6.0",
        "copernicusmarine>=0.8.0",
        "h5py>=3.10.0",
        "scipy>=1.11.0",
        "beautifulsoup4>=4.12.0"
    ]
    
    print("Installing dependencies...")
    
    for package in requirements:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Installed: {package}")
        except subprocess.CalledProcessError:
            print(f"Failed to install: {package}")
    
    print("Dependencies installed")

if __name__ == "__main__":
    sys.exit(main())