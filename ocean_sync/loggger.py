"""
Logging module for Ocean Data Pipeline
"""

import logging
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

class PipelineLogger:
    """Structured logging for data pipeline"""
    
    def __init__(self, log_dir: Path, log_level: str = "INFO"):
        self.log_dir = log_dir
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Log files
        self.text_log = log_dir / f"pipeline_{self.session_id}.log"
        self.csv_log = log_dir / f"pipeline_{self.session_id}_log.csv"
        self.json_metadata = log_dir / f"pipeline_{self.session_id}_metadata.json"
        
        # Log entries storage
        self.log_entries: List[Dict[str, Any]] = []
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Log startup
        self.log("SYSTEM", "INITIALIZATION", "STARTED", "Pipeline logger initialized")
    
    def _setup_logging(self, log_level: str):
        """Configure logging handlers"""
        # Convert string level to logging constant
        level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Clear any existing handlers
        logging.getLogger().handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler
        file_handler = logging.FileHandler(self.text_log)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)
        
        # Configure root logger
        logging.basicConfig(
            level=level,
            handlers=[file_handler, console_handler]
        )
        
        self.logger = logging.getLogger("OceanDataPipeline")
    
    def log(self, source: str, operation: str, status: str, details: str = ""):
        """Log an event with structured format"""
        timestamp = datetime.now()
        
        # Create log entry
        entry = {
            "timestamp": timestamp.isoformat(),
            "source": source,
            "operation": operation,
            "status": status,
            "details": details
        }
        
        # Add to storage
        self.log_entries.append(entry)
        
        # Log to file/console
        log_message = f"{source} | {operation} | {status}"
        if details:
            log_message += f" | {details}"
        
        if status == "ERROR":
            self.logger.error(log_message)
        elif status == "WARNING":
            self.logger.warning(log_message)
        elif status == "SUCCESS":
            self.logger.info(log_message)
        else:
            self.logger.info(log_message)
        
        # Console output
        status_symbol = "[✓]" if status == "SUCCESS" else "[✗]" if status == "ERROR" else "[i]"
        print(f"{status_symbol} {timestamp.strftime('%H:%M:%S')} {source}: {operation} - {status}")
        if details and status in ["ERROR", "WARNING"]:
            print(f"   Details: {details}")
    
    def save_logs(self):
        """Save all logs to files"""
        try:
            # Save CSV log
            if self.log_entries:
                log_df = pd.DataFrame(self.log_entries)
                log_df.to_csv(self.csv_log, index=False)
                print(f"CSV log saved: {self.csv_log}")
            
            # Save metadata
            metadata = {
                "session_id": self.session_id,
                "execution_date": datetime.now().isoformat(),
                "total_entries": len(self.log_entries),
                "error_count": len([e for e in self.log_entries if e["status"] == "ERROR"]),
                "success_count": len([e for e in self.log_entries if e["status"] == "SUCCESS"]),
                "log_files": {
                    "text_log": str(self.text_log),
                    "csv_log": str(self.csv_log),
                    "json_metadata": str(self.json_metadata)
                }
            }
            
            with open(self.json_metadata, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"Metadata saved: {self.json_metadata}")
            
        except Exception as e:
            print(f"Failed to save logs: {e}")
    
    def log_data_preview(self, df, source: str, stage: str):
        """Log data preview"""
        if df is not None and not df.empty:
            self.log(source, "DATA_PREVIEW", "INFO", 
                   f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
            self.log(source, "DATA_PREVIEW", "INFO",
                   f"Columns: {list(df.columns)}")