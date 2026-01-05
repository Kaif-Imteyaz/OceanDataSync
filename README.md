# OceanDataSync Data Pipeline

OceanDataSync is an automated data pipeline for synchronizing oceanographic data from multiple authoritative sources into a unified, standardized format. The system collects, processes, and stores data from NOAA, Copernicus Marine, Argo floats, and NCEI with comprehensive error handling and logging capabilities.

## Features
- Multi-source data synchronization from oceanographic repositories
- Automated data processing and standardization workflows
- Chunked file processing with configurable limits
- Comprehensive logging system with CSV, JSON, and text outputs
- Configurable through YAML settings and environment variables
- Robust error handling with retry mechanisms
- Cross-platform compatibility 

## Potential Applications

OceanDataSync is designed to support:

**Research & Science**
- Climate change and ocean warming studies
- Marine ecosystem and biodiversity research
- Oceanographic modeling and predictions

**Environmental**
- Coastal water quality monitoring
- Marine protected area tracking
- Pollution impact assessment

**Commercial**
- Shipping route optimization
- Offshore wind/wave energy planning
- Aquaculture site selection

**Education**
- Teaching data science with real ocean data
- Student research projects
- Oceanography course materials

**Data & Analytics**
- Machine learning model training
- Time-series pattern analysis
- Building ocean monitoring dashboards


## Data Sources Synchronized

### 1. **NOAA ERDDAP** — [visit](https://coastwatch.pfeg.noaa.gov/erddap)
### 2. **Copernicus Marine Service** — [visit](https://marine.copernicus.eu)
### 3. **Argo Program** — [visit](https://argo.ucsd.edu)
### 4. **NCEI GHCN** (Global Historical Climatology Network) — [visit](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily)

## Project Architecture
```
OceanDataSync/
├── ocean_sync/                  # Core package
│   ├── __init__.py
│   ├── pipeline.py             # Main pipeline orchestrator
│   ├── synchronizer.py         # Data synchronization module
│   ├── processor.py            # Data processing
│   ├── logger.py               # Logging and monitoring sys
│   ├── config.py               # Configuration management
│   
├── scripts/                    # Command-line interface
│   ├── run_sync.py            # Main execution script
│   └── setup_environment.py   # Environment configuration
├── data/                       # Data storage
│   ├── raw/                   # Original downloaded data
│   └── synchronized/          # Processed and synchronized
├── logs/                      # Execution logs and metadata
├── config/                    # Configuration files
│   └── settings.yaml         # Pipeline configuration
├── requirements.txt           # Python dependencies
├── .env.example              # Environment template
└── README.md                 # Documentation
```

## Flow DIagram
![Flow Diagram](OceanSync.drawio.svg)

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager
- Internet connectivity for data synchronization

### Quick Setup
1. Create project directory:
```bash
mkdir OceanDataSync
cd OceanDataSync
```

2. Run environment setup:
```bash
python scripts/setup_environment.py
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

### Environment Setup
Copy `.env.example` to `.env` and configure:
```env
# Data source credentials (optional)
COPERNICUS_USERNAME=your_username
COPERNICUS_PASSWORD=your_password

# Data collection parameters
NOAA_REGION_LAT_MIN=32.0
NOAA_REGION_LAT_MAX=35.0
NOAA_REGION_LON_MIN=-120.0
NOAA_REGION_LON_MAX=-115.0

# Processing settings
MAX_ROWS_PER_FILE=9000
REQUEST_TIMEOUT=30
```

### Pipeline Configuration
Modify `config/settings.yaml`:
```yaml
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
  chunk_size: 9000
  cleanup_temp_files: true

logging:
  level: INFO
  save_csv: true
  save_metadata: true
```

## Usage

### Synchronize All Data Sources
```bash
python scripts/run_sync.py
```

### Synchronize Specific Sources
```bash
python scripts/run_sync.py --sources noaa copernicus
```

### Custom Project Directory
```bash
python scripts/run_sync.py --path "C:/your/custom/path"
```

### Install Dependencies
```bash
python scripts/run_sync.py --install
```

## Output Structure

### Synchronized Data
Processed data is organized by source in `data/synchronized/`:
- `noaa/`: NOAA sea surface temperature data in standardized CSV format
- `copernicus/`: Copernicus Marine SST observations  
- `argo/`: Argo float profile metadata and locations
- `ncei/`: NCEI station metadata and climate data

All files are automatically chunked to 9000 rows maximum for compatibility with various data systems.

### Logging System
- `logs/sync_[timestamp].log`: Detailed text execution log
- `logs/sync_[timestamp]_log.csv`: Structured CSV log for analysis
- `logs/sync_[timestamp]_metadata.json`: JSON metadata with execution statistics

## Error Handling and Monitoring
The pipeline includes comprehensive error management:
- Network timeout handling with automatic retries
- Data validation and cleaning procedures
- Graceful failure with detailed error logging
- Partial success tracking and reporting

## Development

### Extending Data Sources
To add new data sources:
1. Extend the `Synchronizer` class with new synchronization methods
2. Add processing logic to the `Processor` class
3. Update configuration schema
4. Update the requirements.txt if new packages are needed

### Code Standards
- Follow PEP 8 guidelines
- Use type hints for function signatures
- Include comprehensive docstrings
- Maintain modular architecture with clear separation of concerns

## Performance Characteristics
- Typical synchronization time: 30-60 seconds
- Memory usage: < 500MB for standard operations
- Network bandwidth: 100-200MB per complete synchronization
- Storage requirements: 50-100MB for processed data

## Limitations and Considerations
- Requires consistent internet connectivity
- Some data sources may have rate limits
- Large historical datasets may require significant storage
- Real-time data may have availability delays

## Future Enhancements
- Additional data source integrations
- Data quality metrics and validation
- Visualization and reporting capabilities
- Database storage backend options
- Docker containerization
- Cloud deployment configurations

## Data Acknowledgments
- NOAA ERDDAP for ocean data services
- Copernicus Marine Service for global ocean observations
- Argo Program for autonomous float data
- NCEI for climate and historical data archives


### Log Analysis
All execution details are logged in the `logs/` directory:
- Text logs for human-readable execution details
- CSV logs for programmatic analysis
- JSON metadata for system integration


## License
MIT License. See LICENSE file for complete details.

## Version
Current version: 1.0.0


---


