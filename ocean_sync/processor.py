"""
Data processing and transformation module
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any
# import xarray as xr
try:
    import xarray as xr
    XARRAY_AVAILABLE = True
except ImportError:
    XARRAY_AVAILABLE = False
    print("Xarray not available, skipping NetCDF features")

from .config import PipelineConfig
from .logger import PipelineLogger

class DataProcessor:
    """Processes and transforms raw data into standardized format"""
    
    def __init__(self, config: PipelineConfig, logger: PipelineLogger):
        self.config = config
        self.logger = logger
        self.max_rows = config.max_rows_per_file
    
    def process_all(self, raw_files: List[Path]) -> Dict[str, List[Path]]:
        """Process all raw files"""
        processed_files = {
            "noaa": [],
            "copernicus": [],
            "argo": [],
            "ncei": []
        }
        
        for raw_file in raw_files:
            filename = raw_file.name.lower()
            
            self.logger.log("PROCESSOR", "FILE_PROCESSING", "STARTED", f"Processing: {filename}")
            
            try:
                if "noaa" in filename or "jpl" in filename:
                    files = self.process_noaa_file(raw_file)
                    processed_files["noaa"].extend(files)
                
                elif "copernicus" in filename:
                    files = self.process_copernicus_file(raw_file)
                    processed_files["copernicus"].extend(files)
                
                elif "argo" in filename:
                    files = self.process_argo_file(raw_file)
                    processed_files["argo"].extend(files)
                
                elif "ghcnd" in filename or "stations" in filename:
                    files = self.process_ncei_file(raw_file)
                    processed_files["ncei"].extend(files)
                
                else:
                    self.logger.log("PROCESSOR", "FILE_PROCESSING", "WARNING",
                                  f"Unknown file type: {filename}")
            
            except Exception as e:
                self.logger.log("PROCESSOR", "FILE_PROCESSING", "ERROR",
                              f"Failed to process {filename}: {e}")
        
        return processed_files
    
    def process_noaa_file(self, filepath: Path) -> List[Path]:
        """Process NOAA SST CSV file"""
        self.logger.log("NOAA", "PROCESSING", "STARTED")
        
        try:
            # Read CSV
            df = pd.read_csv(filepath)
            
            # Standardize column names
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower()
                if "time" in col_lower:
                    column_mapping[col] = "timestamp"
                elif "lat" in col_lower:
                    column_mapping[col] = "latitude"
                elif "lon" in col_lower:
                    column_mapping[col] = "longitude"
                elif "sst" in col_lower or "analysed" in col_lower:
                    column_mapping[col] = "sea_surface_temperature"
            
            df = df.rename(columns=column_mapping)
            
            # Select and reorder columns
            required_cols = ["timestamp", "latitude", "longitude", "sea_surface_temperature"]
            available_cols = [col for col in required_cols if col in df.columns]
            
            if not available_cols:
                raise ValueError("No recognizable columns found")
            
            result = df[available_cols].copy()
            
            # Convert temperature from Kelvin to Celsius if needed
            if "sea_surface_temperature" in result.columns:
                # Check if values are in Kelvin (typically > 200)
                if result["sea_surface_temperature"].mean() > 200:
                    result["sea_surface_temperature"] = result["sea_surface_temperature"] - 273.15
                    result.rename(columns={"sea_surface_temperature": "sst_celsius"}, inplace=True)
            
            # Clean data
            result = result.dropna(subset=["latitude", "longitude"])
            
            self.logger.log_data_preview(result.head(), "NOAA", "PROCESSED")
            
            # Save processed files
            output_files = self._save_chunked(result, filepath, self.config.noaa_dir, "noaa")
            
            self.logger.log("NOAA", "PROCESSING", "SUCCESS",
                          f"Created {len(output_files)} file(s)")
            
            return output_files
            
        except Exception as e:
            self.logger.log("NOAA", "PROCESSING", "ERROR", str(e))
            return []
    
    def process_copernicus_file(self, filepath: Path) -> List[Path]:
        """Process Copernicus SST data"""
        self.logger.log("COPERNICUS", "PROCESSING", "STARTED")
        
        try:
            # Read CSV
            df = pd.read_csv(filepath)
            
            # Standardize columns
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower()
                if any(time_word in col_lower for time_word in ["time", "date"]):
                    column_mapping[col] = "timestamp"
                elif "lat" in col_lower:
                    column_mapping[col] = "latitude"
                elif "lon" in col_lower:
                    column_mapping[col] = "longitude"
                elif any(temp_word in col_lower for temp_word in ["sst", "temp", "analysed"]):
                    column_mapping[col] = "sea_surface_temperature"
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns
            required_cols = ["timestamp", "latitude", "longitude", "sea_surface_temperature"]
            
            # Add missing columns with None
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None
            
            result = df[required_cols].copy()
            
            # Convert temperature if in Kelvin
            if "sea_surface_temperature" in result.columns:
                if result["sea_surface_temperature"].mean() > 200:
                    result["sea_surface_temperature"] = result["sea_surface_temperature"] - 273.15
            
            # Clean
            result = result.dropna(subset=["latitude", "longitude"])
            
            self.logger.log_data_preview(result.head(), "COPERNICUS", "PROCESSED")
            
            output_files = self._save_chunked(result, filepath, self.config.copernicus_dir, "copernicus")
            
            self.logger.log("COPERNICUS", "PROCESSING", "SUCCESS",
                          f"Created {len(output_files)} file(s)")
            
            return output_files
            
        except Exception as e:
            self.logger.log("COPERNICUS", "PROCESSING", "ERROR", str(e))
            return []
    
    def process_argo_file(self, filepath: Path) -> List[Path]:
        """Process Argo profile index"""
        self.logger.log("ARGO", "PROCESSING", "STARTED")
        
        try:
            # Read Argo index file (space-separated)
            df = pd.read_csv(filepath, sep=r'\s+', comment='#', header=None,
                           names=['file', 'date', 'latitude', 'longitude', 'ocean', 'prof_type',
                                  'institution', 'date_update'], low_memory=False)
            
            # Convert date to datetime
            df['timestamp'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M%S', errors='coerce')
            
            # Select and clean columns
            result = df[['timestamp', 'latitude', 'longitude', 'ocean', 'prof_type', 'institution']].copy()
            
            # Convert to numeric
            result['latitude'] = pd.to_numeric(result['latitude'], errors='coerce')
            result['longitude'] = pd.to_numeric(result['longitude'], errors='coerce')
            
            # Remove rows without coordinates
            result = result.dropna(subset=['latitude', 'longitude'])
            
            # Add temperature column (would need actual profile data)
            result['temperature_celsius'] = None
            
            self.logger.log_data_preview(result.head(), "ARGO", "PROCESSED")
            
            output_files = self._save_chunked(result, filepath, self.config.argo_dir, "argo")
            
            self.logger.log("ARGO", "PROCESSING", "SUCCESS",
                          f"Created {len(output_files)} file(s) with {len(result)} profiles")
            
            return output_files
            
        except Exception as e:
            self.logger.log("ARGO", "PROCESSING", "ERROR", str(e))
            return []
    
    def process_ncei_file(self, filepath: Path) -> List[Path]:
        """Process NCEI GHCN station data"""
        self.logger.log("NCEI", "PROCESSING", "STARTED")
        
        try:
            # Read fixed-width station file
            colspecs = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71)]
            df = pd.read_fwf(filepath, colspecs=colspecs,
                           names=['station_id', 'latitude', 'longitude', 
                                  'elevation', 'state', 'name'])
            
            # Convert to numeric
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            df['elevation'] = pd.to_numeric(df['elevation'], errors='coerce')
            
            # Create standardized result
            result = pd.DataFrame({
                'station_id': df['station_id'],
                'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d'),
                'latitude': df['latitude'],
                'longitude': df['longitude'],
                'elevation_m': df['elevation'],
                'country': df['state'].str[:2] if 'state' in df.columns else None,
                'station_name': df['name'] if 'name' in df.columns else None
            })
            
            # Remove rows without coordinates
            result = result.dropna(subset=['latitude', 'longitude'])
            
            self.logger.log_data_preview(result.head(), "NCEI", "PROCESSED")
            
            output_files = self._save_chunked(result, filepath, self.config.ncei_dir, "ncei")
            
            self.logger.log("NCEI", "PROCESSING", "SUCCESS",
                          f"Created {len(output_files)} file(s) with {len(result)} stations")
            
            return output_files
            
        except Exception as e:
            self.logger.log("NCEI", "PROCESSING", "ERROR", str(e))
            return []
    
    def _save_chunked(self, df: pd.DataFrame, original_file: Path, 
                     output_dir: Path, source_prefix: str) -> List[Path]:
        """Save DataFrame in chunks if it exceeds max rows"""
        if len(df) == 0:
            self.logger.log("PROCESSOR", "SAVE", "WARNING", "Empty DataFrame, nothing to save")
            return []
        
        output_files = []
        base_name = original_file.stem
        
        if len(df) <= self.max_rows:
            # Save single file
            output_file = output_dir / f"{source_prefix}_{base_name}_processed.csv"
            df.to_csv(output_file, index=False)
            output_files.append(output_file)
            
            self.logger.log("PROCESSOR", "SAVE", "INFO",
                          f"Saved {len(df)} rows to {output_file.name}")
        else:
            # Split into chunks
            num_chunks = (len(df) + self.max_rows - 1) // self.max_rows
            chunks = np.array_split(df, num_chunks)
            
            self.logger.log("PROCESSOR", "SAVE", "INFO",
                          f"Splitting {len(df)} rows into {num_chunks} files")
            
            for i, chunk in enumerate(chunks, 1):
                output_file = output_dir / f"{source_prefix}_{base_name}_processed_part{i:03d}.csv"
                chunk.to_csv(output_file, index=False)
                output_files.append(output_file)
                
                self.logger.log("PROCESSOR", "SAVE", "INFO",
                              f"Saved part {i}: {len(chunk)} rows")
        
        return output_files