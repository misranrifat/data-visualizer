#!/usr/bin/env python3
"""
Data Visualizer - Startup Script

This script starts the Flask data visualizer application with comprehensive logging.
"""

import sys
import os
import logging
from datetime import datetime

def setup_startup_logging():
    """Setup console-only logging for the startup script"""
    # Create startup logger
    logger = logging.getLogger('startup')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | STARTUP | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Add only console handler to logger
    logger.addHandler(console_handler)
    
    return logger

def check_dependencies():
    """Check if required dependencies are installed"""
    logger = logging.getLogger('startup')
    missing_deps = []
    
    logger.info("DEPENDENCY_CHECK: Starting dependency validation")
    
    try:
        import flask
        logger.info("DEPENDENCY_CHECK: Flask - OK")
    except ImportError:
        missing_deps.append('flask')
        logger.error("DEPENDENCY_CHECK: Flask - MISSING")
    
    try:
        import pandas
        logger.info("DEPENDENCY_CHECK: Pandas - OK")
    except ImportError:
        missing_deps.append('pandas')
        logger.error("DEPENDENCY_CHECK: Pandas - MISSING")
    
    try:
        import dask
        logger.info("DEPENDENCY_CHECK: Dask - OK")
    except ImportError:
        missing_deps.append('dask')
        logger.error("DEPENDENCY_CHECK: Dask - MISSING")
    
    try:
        import boto3
        logger.info("DEPENDENCY_CHECK: Boto3 - OK")
    except ImportError:
        missing_deps.append('boto3')
        logger.error("DEPENDENCY_CHECK: Boto3 - MISSING")
    
    if missing_deps:
        logger.error("DEPENDENCY_CHECK: Missing critical dependencies:")
        for dep in missing_deps:
            logger.error(f"DEPENDENCY_CHECK:    - {dep}")
        logger.error("DEPENDENCY_CHECK: Install missing dependencies with:")
        logger.error("DEPENDENCY_CHECK:    pip install -r requirements.txt")
        return False
    
    logger.info("DEPENDENCY_CHECK: All required dependencies found")
    return True

def check_optional_dependencies():
    """Check for optional dependencies and log their status"""
    logger = logging.getLogger('startup')
    
    logger.info("OPTIONAL_CHECK: Checking optional dependencies")
    
    try:
        import pyarrow
        logger.info("OPTIONAL_CHECK: PyArrow - OK (Parquet support enabled)")
    except ImportError:
        logger.warning("OPTIONAL_CHECK: PyArrow - MISSING (Only CSV support available)")
        logger.warning("OPTIONAL_CHECK:    Install with: pip install pyarrow")
    
    try:
        import s3fs
        logger.info("OPTIONAL_CHECK: s3fs - OK (S3 integration enabled)")
    except ImportError:
        logger.warning("OPTIONAL_CHECK: s3fs - MISSING (S3 integration disabled)")
        logger.warning("OPTIONAL_CHECK:    Install with: pip install s3fs")
    
    try:
        import faker
        logger.info("OPTIONAL_CHECK: Faker - OK (Mock data generation enabled)")
    except ImportError:
        logger.warning("OPTIONAL_CHECK: Faker - MISSING (Mock data generation disabled)")
        logger.warning("OPTIONAL_CHECK:    Install with: pip install faker")

def main():
    """Main startup function"""
    # Initialize logging first
    logger = setup_startup_logging()
    
    logger.info("=" * 80)
    logger.info("DATA VISUALIZER - APPLICATION STARTUP")
    logger.info("=" * 80)
    logger.info(f"STARTUP_TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"PYTHON_VERSION: {sys.version}")
    logger.info(f"WORKING_DIRECTORY: {os.getcwd()}")
    
    # Check dependencies
    logger.info("STARTUP_PHASE: Dependency validation")
    if not check_dependencies():
        logger.error("STARTUP_FAILED: Missing required dependencies")
        sys.exit(1)
    
    # Check optional dependencies
    logger.info("STARTUP_PHASE: Optional dependency check")
    check_optional_dependencies()
    
    # Log available features
    logger.info("STARTUP_PHASE: Feature availability check")
    logger.info("FEATURES_AVAILABLE:")
    logger.info("   • CSV file upload (drag & drop)")
    logger.info("   • Data preview and statistics")
    logger.info("   • Dataset information summary")
    logger.info("   • Professional logging system")
    logger.info("   • Comprehensive error handling")
    
    # Check for optional features
    try:
        import pyarrow
        logger.info("   • Parquet file support")
    except ImportError:
        pass
    
    try:
        import s3fs
        logger.info("   • S3 integration (CSV and Parquet)")
    except ImportError:
        pass
    
    try:
        import faker
        logger.info("   • Mock data generation")
    except ImportError:
        pass
    
    logger.info("STARTUP_PHASE: Flask server initialization")
    logger.info("SERVER_CONFIG:")
    logger.info("   • URL: http://localhost:5000")
    logger.info("   • Host: 0.0.0.0")
    logger.info("   • Port: 5000")
    logger.info("   • Debug: True")
    logger.info("   • Press Ctrl+C to stop")
    
    logger.info("=" * 80)
    logger.info("STARTUP_COMPLETE: Launching Flask application")
    
    # Import and run the app
    try:
        from app import app
        logger.info("APPLICATION_IMPORT: Flask app imported successfully")
        logger.info("APPLICATION_START: Starting Flask development server")
        
        # Disable Flask/Werkzeug HTTP request logs
        werkzeug_logger = logging.getLogger('werkzeug')
        werkzeug_logger.setLevel(logging.ERROR)
        
        app.run(debug=True, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        logger.info("APPLICATION_STOP: Application stopped by user (Ctrl+C)")
        logger.info("SHUTDOWN_COMPLETE: Data Visualizer application terminated")
    except Exception as e:
        logger.error(f"APPLICATION_ERROR: Error starting application: {e}")
        logger.error("STARTUP_FAILED: Application failed to start")
        sys.exit(1)

if __name__ == '__main__':
    main() 