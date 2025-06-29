import os
import logging
import logging.handlers
import traceback
from datetime import datetime
from functools import wraps
from urllib.parse import unquote
import dask.dataframe as dd
from flask import Flask, render_template, request, jsonify, send_from_directory
from botocore.exceptions import NoCredentialsError, ClientError
from werkzeug.utils import secure_filename
import psutil
import warnings
warnings.filterwarnings('ignore')

# Configure console-only logging system
def setup_logging():
    """Setup console-only logging configuration"""
    # Create main logger
    logger = logging.getLogger('data_visualizer')
    logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create console formatter
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add only console handler to logger
    logger.addHandler(console_handler)
    
    return logger

# Initialize console-only logging
logger = setup_logging()

# Disable Flask/Werkzeug HTTP request logs
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)  # Only show errors, not INFO level HTTP requests

# Custom exception classes
class DataVisualizerError(Exception):
    """Base exception for Data Visualizer application"""
    def __init__(self, message, error_code=None, details=None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}
        self.timestamp = datetime.utcnow().isoformat()

class FileProcessingError(DataVisualizerError):
    """Exception raised for file processing errors"""
    pass

class S3AccessError(DataVisualizerError):
    """Exception raised for S3 access errors"""
    pass

class ValidationError(DataVisualizerError):
    """Exception raised for validation errors"""
    pass

# Logging decorators
def log_function_call(func):
    """Decorator to log function calls with parameters and execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.utcnow()
        func_name = func.__name__
        
        # Log function entry
        logger.info(f"FUNCTION_START: {func_name}")
        logger.debug(f"FUNCTION_PARAMS: {func_name} - args: {len(args)}, kwargs: {list(kwargs.keys())}")
        
        try:
            result = func(*args, **kwargs)
            
            # Calculate execution time
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"FUNCTION_SUCCESS: {func_name} - executed in {execution_time:.3f}s")
            
            return result
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"FUNCTION_ERROR: {func_name} - failed after {execution_time:.3f}s - {str(e)}")
            logger.error(f"FUNCTION_TRACEBACK: {func_name} - {traceback.format_exc()}")
            raise
    
    return wrapper

def log_api_request(func):
    """Decorator to log API requests with comprehensive details"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.utcnow()
        
        # Log request details
        # logger.info(f"API_REQUEST_START: {request.method} {request.endpoint}")
        # logger.info(f"API_REQUEST_IP: {request.remote_addr}")
        # logger.info(f"API_REQUEST_USER_AGENT: {request.headers.get('User-Agent', 'Unknown')}")
        
        if request.method == 'POST':
            content_type = request.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                try:
                    logger.debug(f"API_REQUEST_JSON: {request.get_json()}")
                except:
                    logger.debug("API_REQUEST_JSON: Failed to parse JSON body")
            elif 'multipart/form-data' in content_type:
                logger.debug(f"API_REQUEST_FORM: files={list(request.files.keys())}, form={list(request.form.keys())}")
        
        try:
            response = func(*args, **kwargs)
            
            # Calculate response time
            response_time = (datetime.utcnow() - start_time).total_seconds()
            
            # Log successful response
            if hasattr(response, 'status_code'):
                status_code = response.status_code
            else:
                status_code = 200  # Default for successful responses
            
            # logger.info(f"API_REQUEST_SUCCESS: {request.method} {request.endpoint} - {status_code} - {response_time:.3f}s")
            
            return response
            
        except Exception as e:
            response_time = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"API_REQUEST_ERROR: {request.method} {request.endpoint} - failed after {response_time:.3f}s")
            logger.error(f"API_REQUEST_EXCEPTION: {str(e)}")
            logger.error(f"API_REQUEST_TRACEBACK: {traceback.format_exc()}")
            raise
    
    return wrapper

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
# No file size limit

# Log application startup
logger.info("APPLICATION_STARTUP: Data Visualizer Flask application starting")
logger.info(f"APPLICATION_CONFIG: Upload folder: {app.config['UPLOAD_FOLDER']}")

# Create uploads directory if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
logger.info(f"DIRECTORY_CREATED: {app.config['UPLOAD_FOLDER']}")

# Check for optional dependencies
try:
    import pyarrow as pa
    import s3fs
    PARQUET_AVAILABLE = True
    S3FS_AVAILABLE = True
    logger.info("DEPENDENCIES_CHECK: PyArrow and s3fs available - Full functionality enabled")
except ImportError as e:
    PARQUET_AVAILABLE = False
    S3FS_AVAILABLE = False
    logger.warning(f"DEPENDENCIES_CHECK: Optional dependencies missing - {str(e)}")

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv', 'parquet'}
logger.info(f"FILE_EXTENSIONS: Allowed extensions: {ALLOWED_EXTENSIONS}")

@log_function_call
def allowed_file(filename):
    """Check if file extension is allowed"""
    try:
        if not filename or '.' not in filename:
            logger.warning(f"FILE_VALIDATION: Invalid filename format: {filename}")
            return False
        
        extension = filename.rsplit('.', 1)[1].lower()
        is_allowed = extension in ALLOWED_EXTENSIONS
        
        logger.debug(f"FILE_VALIDATION: {filename} - extension: {extension} - allowed: {is_allowed}")
        return is_allowed
        
    except Exception as e:
        logger.error(f"FILE_VALIDATION_ERROR: Error checking file {filename} - {str(e)}")
        return False

@log_function_call
def parse_s3_url(s3_url):
    """Parse S3 URL to extract bucket name and object path, detecting if it's a file or folder"""
    try:
        # Remove any leading/trailing whitespace
        s3_url = s3_url.strip()
        
        # Handle different S3 URL formats
        if s3_url.startswith('s3://'):
            # Format: s3://bucket-name/folder/path/ or s3://bucket-name/folder/file.ext
            url_without_protocol = s3_url[5:]  # Remove 's3://'
        elif s3_url.startswith('https://s3.amazonaws.com/'):
            # Format: https://s3.amazonaws.com/bucket-name/folder/path/
            url_without_protocol = s3_url[25:]  # Remove 'https://s3.amazonaws.com/'
        elif s3_url.startswith('https://') and '.s3.amazonaws.com/' in s3_url:
            # Format: https://bucket-name.s3.amazonaws.com/folder/path/
            # Extract bucket from subdomain and path
            parts = s3_url.replace('https://', '').split('.s3.amazonaws.com/')
            if len(parts) == 2:
                bucket_name = parts[0]
                object_path = _normalize_s3_path(parts[1])
                # Determine if it's a file or folder
                is_file = _is_s3_file_path(object_path)
                logger.debug(f"S3_URL_PARSED: {s3_url} -> bucket: {bucket_name}, path: {object_path}, is_file: {is_file}")
                return bucket_name, object_path, is_file
            else:
                raise ValidationError("Invalid S3 URL format", "INVALID_S3_URL")
        elif s3_url.startswith('https://') and '.s3.' in s3_url and '.amazonaws.com/' in s3_url:
            # Format: https://bucket-name.s3.region.amazonaws.com/folder/path/
            # Extract bucket from subdomain and path (regional format)
            url_without_https = s3_url.replace('https://', '')
            
            # Find the pattern: bucket-name.s3.region.amazonaws.com/path
            if '.s3.' in url_without_https and '.amazonaws.com/' in url_without_https:
                # Split at .s3. to get bucket name
                bucket_part = url_without_https.split('.s3.')[0]
                # Split at .amazonaws.com/ to get the path part
                path_part = url_without_https.split('.amazonaws.com/')
                if len(path_part) == 2:
                    bucket_name = bucket_part
                    object_path = _normalize_s3_path(path_part[1])
                    # Determine if it's a file or folder
                    is_file = _is_s3_file_path(object_path)
                    logger.debug(f"S3_URL_PARSED: {s3_url} -> bucket: {bucket_name}, path: {object_path}, is_file: {is_file}")
                    return bucket_name, object_path, is_file
                else:
                    raise ValidationError("Invalid regional S3 URL format", "INVALID_S3_URL")
            else:
                raise ValidationError("Invalid regional S3 URL format", "INVALID_S3_URL")
        else:
            raise ValidationError("S3 URL must start with 's3://' or be a valid HTTPS S3 URL", "INVALID_S3_URL")
        
        # Split bucket and object path
        if '/' in url_without_protocol:
            parts = url_without_protocol.split('/', 1)
            bucket_name = parts[0]
            object_path = parts[1]
        else:
            # Only bucket name provided
            bucket_name = url_without_protocol
            object_path = ""
        
        # Validate bucket name
        if not bucket_name:
            raise ValidationError("Bucket name cannot be empty", "EMPTY_BUCKET_NAME")
        
        # Normalize the object path (handle URL encoding)
        object_path = _normalize_s3_path(object_path)
        
        # Determine if it's a file or folder
        is_file = _is_s3_file_path(object_path)
        
        logger.debug(f"S3_URL_PARSED: {s3_url} -> bucket: {bucket_name}, path: {object_path}, is_file: {is_file}")
        return bucket_name, object_path, is_file
        
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        logger.error(f"S3_URL_PARSE_ERROR: {s3_url} - {str(e)}")
        raise ValidationError(f"Error parsing S3 URL: {str(e)}", "S3_URL_PARSE_ERROR")

@log_function_call
def _is_s3_file_path(path):
    """Determine if an S3 path points to a file or folder"""
    if not path:
        return False
    
    # If path ends with '/', it's definitely a folder
    if path.endswith('/'):
        return False
    
    # Check if the last part has a file extension
    path_parts = path.split('/')
    last_part = path_parts[-1]
    
    # Check for common file extensions
    file_extensions = {'.csv', '.parquet', '.json', '.txt', '.tsv', '.xlsx', '.xls'}
    for ext in file_extensions:
        if last_part.lower().endswith(ext):
            return True
    
    # If no clear extension, assume it's a folder
    return False

@log_function_call
def _normalize_s3_path(path):
    """Normalize S3 path by URL decoding and handling common encoding issues"""
    if not path:
        return path
    
    try:
        # URL decode the path to handle %3D (=) and other encoded characters
        decoded_path = unquote(path)
        
        # Log if decoding was needed
        if decoded_path != path:
            logger.debug(f"S3_PATH_DECODE: '{path}' -> '{decoded_path}'")
        
        return decoded_path
    except Exception as e:
        logger.warning(f"S3_PATH_DECODE_ERROR: Could not decode path '{path}': {str(e)}")
        return path

@log_function_call
def _detect_partitioned_structure(path):
    """Detect if a path contains Hive-style partitioning (key=value patterns)"""
    if not path:
        return False, {}
    
    path_parts = path.strip('/').split('/')
    partitions = {}
    
    for part in path_parts:
        if '=' in part:
            key, value = part.split('=', 1)
            partitions[key] = value
    
    is_partitioned = len(partitions) > 0
    logger.debug(f"PARTITION_DETECTION: path={path}, is_partitioned={is_partitioned}, partitions={partitions}")
    
    return is_partitioned, partitions

@log_function_call
def _build_partition_filter_path(bucket_name, base_path, partitions):
    """Build an S3 path pattern for reading partitioned data"""
    # Remove trailing slash and split path
    clean_path = base_path.rstrip('/')
    path_parts = clean_path.split('/')
    
    # Find the base path (before partition directories)
    base_parts = []
    partition_parts = []
    found_partitions = False
    
    for part in path_parts:
        if '=' in part:
            found_partitions = True
            partition_parts.append(part)
        elif not found_partitions:
            base_parts.append(part)
        else:
            # Part after partitions (like filename)
            partition_parts.append(part)
    
    # Construct the base path for partitioned reading
    if base_parts:
        partition_base = '/'.join(base_parts)
    else:
        partition_base = ''
    
    logger.debug(f"PARTITION_PATH_BUILD: base={partition_base}, partitions={partition_parts}")
    return partition_base

@log_function_call
def read_file_data_dask(file_path, file_type, rows_limit='all'):
    """Read data using Dask for large files with optional row limit"""
    try:
        logger.info(f"FILE_READ_START: {file_path} - type: {file_type} - limit: {rows_limit}")
        
        if not os.path.exists(file_path):
            raise FileProcessingError(f"File not found: {file_path}", "FILE_NOT_FOUND")
        
        file_size = os.path.getsize(file_path)
        logger.info(f"FILE_INFO: {file_path} - size: {file_size:,} bytes")
        
        if file_type == 'csv':
            logger.debug("FILE_READ: Using Dask CSV reader")
            ddf = dd.read_csv(file_path, assume_missing=True)
        elif file_type == 'parquet':
            if not PARQUET_AVAILABLE:
                raise FileProcessingError(
                    "Parquet support not available. Please install pyarrow: pip install pyarrow",
                    "PARQUET_NOT_AVAILABLE"
                )
            logger.debug("FILE_READ: Using Dask Parquet reader")
            ddf = dd.read_parquet(file_path)
        else:
            raise ValidationError(f"Unsupported file type: {file_type}", "UNSUPPORTED_FILE_TYPE")
        
        # Apply row limit if specified
        if rows_limit != 'all':
            nrows = int(rows_limit)
            logger.info(f"FILE_READ: Applying row limit: {nrows:,}")
            df = ddf.head(nrows, npartitions=-1)
        else:
            logger.info("FILE_READ: Loading complete dataset")
            df = ddf.compute()
        
        logger.info(f"FILE_READ_SUCCESS: Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df
        
    except Exception as e:
        logger.error(f"FILE_READ_ERROR: {file_path} - {str(e)}")
        if isinstance(e, DataVisualizerError):
            raise
        else:
            raise FileProcessingError(f"Error reading file: {str(e)}", "FILE_READ_FAILED", {"file_path": file_path})

@log_function_call
def read_s3_file_dask(bucket_name, file_path, file_type, sample_size=50000):
    """Read data directly from an individual S3 file using Dask with partition awareness"""
    try:
        logger.info(f"S3_FILE_READ_START: s3://{bucket_name}/{file_path} - type: {file_type} - sample: {sample_size}")
        
        if not S3FS_AVAILABLE:
            raise S3AccessError(
                "S3 support not available. Please install s3fs: pip install s3fs",
                "S3FS_NOT_AVAILABLE"
            )
        
        # Check for partitioned structure
        is_partitioned, partitions = _detect_partitioned_structure(file_path)
        
        # Construct full S3 path for the file
        s3_file_path = f"s3://{bucket_name}/{file_path}"
        logger.debug(f"S3_FILE_PATH: {s3_file_path}")
        
        if is_partitioned:
            logger.info(f"S3_PARTITIONED_FILE: Detected partitions: {partitions}")
        
        # Read the specific file based on type
        if file_type == 'csv':
            logger.debug(f"S3_CSV_FILE: Reading {s3_file_path}")
            try:
                ddf = dd.read_csv(s3_file_path, assume_missing=True)
            except Exception as e:
                logger.warning(f"S3_CSV_FILE_ERROR: {str(e)}")
                raise S3AccessError(
                    f"Failed to read CSV file from S3. Path: {s3_file_path}. Error: {str(e)}",
                    "CSV_FILE_READ_FAILED"
                )
        elif file_type == 'parquet':
            if not PARQUET_AVAILABLE:
                raise S3AccessError(
                    "Parquet support not available. Please install pyarrow: pip install pyarrow",
                    "PARQUET_NOT_AVAILABLE"
                )
            logger.debug(f"S3_PARQUET_FILE: Reading {s3_file_path}")
            try:
                ddf = dd.read_parquet(s3_file_path)
            except Exception as e:
                logger.warning(f"S3_PARQUET_FILE_ERROR: {str(e)}")
                raise S3AccessError(
                    f"Failed to read Parquet file from S3. Path: {s3_file_path}. Error: {str(e)}",
                    "PARQUET_FILE_READ_FAILED"
                )
        else:
            raise ValidationError(f"Unsupported file type: {file_type}", "UNSUPPORTED_FILE_TYPE")
        
        # Handle sample size parameter
        if sample_size == 'all':
            max_safe_rows = None  # No limit
            logger.info(f"S3_FILE_SAMPLING: Loading all available data")
        else:
            max_safe_rows = int(sample_size)
            logger.info(f"S3_FILE_SAMPLING: Limiting to {max_safe_rows:,} rows for safety")
        
        # Get basic info without loading full data
        try:
            total_rows = len(ddf)
            logger.info(f"S3_FILE_SIZE: {total_rows:,} total rows detected")
        except Exception as e:
            logger.warning(f"S3_FILE_SIZE_ESTIMATION: Could not determine exact size - {str(e)}")
            # For single files, we can try to compute a small sample to estimate
            try:
                sample_partition = ddf.get_partition(0).compute()
                estimated_rows = len(sample_partition) * ddf.npartitions
                total_rows = estimated_rows
                logger.info(f"S3_FILE_SIZE_ESTIMATE: ~{total_rows:,} rows estimated")
            except:
                total_rows = 10000  # Conservative fallback estimate
                logger.info(f"S3_FILE_SIZE_FALLBACK: Using fallback estimate of {total_rows:,} rows")
        
        # Sample data based on user selection
        if max_safe_rows is None:
            # Load all data
            sample_df = ddf.compute()
            rows_loaded_info = f"Loaded all {len(sample_df):,} rows from S3 file"
        else:
            # Load limited sample
            sample_df = ddf.head(max_safe_rows, npartitions=-1)
            if total_rows > max_safe_rows:
                rows_loaded_info = f"Loaded sample of {max_safe_rows:,} rows from {total_rows:,} total rows in S3 file"
            else:
                rows_loaded_info = f"Loaded {len(sample_df):,} rows from S3 file"
        
        # Add partition information to the dataframe if partitioned
        if is_partitioned and not sample_df.empty:
            for key, value in partitions.items():
                if key not in sample_df.columns:
                    sample_df[key] = value
                    logger.info(f"S3_PARTITION_COLUMN: Added partition column '{key}' = '{value}'")
            
            partition_info = ", ".join([f"{k}={v}" for k, v in partitions.items()])
            rows_loaded_info += f" (partitioned: {partition_info})"
        
        logger.info(f"S3_FILE_READ_SUCCESS: {rows_loaded_info}")
        return sample_df, rows_loaded_info
        
    except Exception as e:
        logger.error(f"S3_FILE_READ_ERROR: s3://{bucket_name}/{file_path} - {str(e)}")
        if isinstance(e, DataVisualizerError):
            raise
        else:
            raise S3AccessError(f"Error reading S3 file data: {str(e)}", "S3_FILE_READ_FAILED", {
                "bucket": bucket_name,
                "file_path": file_path,
                "file_type": file_type
            })

@log_function_call
def read_s3_data_dask(bucket_name, folder_path, file_type, sample_size=50000):
    """Read data directly from S3 folder using Dask with partition awareness for terabyte-scale processing"""
    try:
        logger.info(f"S3_READ_START: s3://{bucket_name}/{folder_path} - type: {file_type} - sample: {sample_size}")
        
        if not S3FS_AVAILABLE:
            raise S3AccessError(
                "S3 support not available. Please install s3fs: pip install s3fs",
                "S3FS_NOT_AVAILABLE"
            )
        
        # Check for partitioned structure
        is_partitioned, partitions = _detect_partitioned_structure(folder_path)
        
        # Construct S3 path for folder (ensure it ends with /)
        if not folder_path.endswith('/'):
            folder_path += '/'
        s3_path = f"s3://{bucket_name}/{folder_path}"
        
        logger.debug(f"S3_PATH: {s3_path}")
        
        if is_partitioned:
            logger.info(f"S3_PARTITIONED_FOLDER: Detected partitions: {partitions}")
            # For partitioned folders, we can use Dask's built-in partition handling
            
        if file_type == 'csv':
            if is_partitioned:
                # For partitioned CSV, read with recursive pattern
                csv_pattern = f"{s3_path}**/*.csv"
                logger.debug(f"S3_CSV_PARTITIONED_PATTERN: {csv_pattern}")
                try:
                    ddf = dd.read_csv(csv_pattern, assume_missing=True)
                except Exception as e:
                    logger.warning(f"S3_CSV_PARTITIONED_FALLBACK: Recursive pattern failed, trying direct path: {str(e)}")
                    # Fallback to direct path reading
                    csv_pattern = f"{s3_path}*.csv"
                    ddf = dd.read_csv(csv_pattern, assume_missing=True)
            else:
                csv_pattern = f"{s3_path}*.csv"
                logger.debug(f"S3_CSV_PATTERN: {csv_pattern}")
                ddf = dd.read_csv(csv_pattern, assume_missing=True)
        elif file_type == 'parquet':
            if not PARQUET_AVAILABLE:
                raise S3AccessError(
                    "Parquet support not available. Please install pyarrow: pip install pyarrow",
                    "PARQUET_NOT_AVAILABLE"
                )
            logger.debug(f"S3_PARQUET_PATH: {s3_path}")
            # Dask automatically handles partitioned Parquet datasets
            try:
                ddf = dd.read_parquet(s3_path)
            except Exception as e:
                logger.warning(f"S3_PARQUET_READ_ERROR: {str(e)}")
                # Try to provide more specific error handling for URL encoding issues
                if "%" in s3_path:
                    logger.info("S3_PARQUET_RETRY: Detected potential URL encoding issue, path already normalized")
                raise S3AccessError(
                    f"Failed to read Parquet data from S3. This might be due to path encoding issues or missing files. Path: {s3_path}",
                    "PARQUET_READ_FAILED"
                )
        else:
            raise ValidationError(f"Unsupported file type: {file_type}", "UNSUPPORTED_FILE_TYPE")
        
        # Handle sample size parameter
        if sample_size == 'all':
            max_safe_rows = None  # No limit
            logger.info(f"S3_SAMPLING: Loading all available data")
        else:
            max_safe_rows = int(sample_size)
            logger.info(f"S3_SAMPLING: Limiting to {max_safe_rows:,} rows for safety")
        
        # Get basic info without loading full data
        try:
            total_rows = len(ddf)
            logger.info(f"S3_DATASET_SIZE: {total_rows:,} total rows detected")
        except Exception as e:
            logger.warning(f"S3_SIZE_ESTIMATION: Could not determine exact size - {str(e)}")
            estimated_partitions = ddf.npartitions
            total_rows = estimated_partitions * 10000  # Conservative estimate
            logger.info(f"S3_SIZE_ESTIMATE: ~{total_rows:,} rows estimated from {estimated_partitions} partitions")
        
        # Sample data based on user selection
        if max_safe_rows is None:
            # Load all data
            sample_df = ddf.compute()
            rows_loaded_info = f"Loaded all {len(sample_df):,} rows from cloud dataset"
        else:
            # Load limited sample
            sample_df = ddf.head(max_safe_rows, npartitions=-1)
            if total_rows > max_safe_rows:
                rows_loaded_info = f"Loaded sample of {max_safe_rows:,} rows from estimated {total_rows:,} total rows"
            else:
                rows_loaded_info = f"Loaded {len(sample_df):,} rows from cloud dataset"
        
        # Add partition information if detected
        if is_partitioned and not sample_df.empty:
            # Check if partition columns are already present (common with Parquet)
            missing_partitions = {}
            for key, value in partitions.items():
                if key not in sample_df.columns:
                    missing_partitions[key] = value
            
            # Add missing partition columns
            for key, value in missing_partitions.items():
                sample_df[key] = value
                logger.info(f"S3_PARTITION_COLUMN: Added partition column '{key}' = '{value}'")
            
            if partitions:
                partition_info = ", ".join([f"{k}={v}" for k, v in partitions.items()])
                rows_loaded_info += f" (partitioned: {partition_info})"
        
        logger.info(f"S3_READ_SUCCESS: {rows_loaded_info}")
        return sample_df, rows_loaded_info
        
    except Exception as e:
        logger.error(f"S3_READ_ERROR: s3://{bucket_name}/{folder_path} - {str(e)}")
        if isinstance(e, DataVisualizerError):
            raise
        else:
            raise S3AccessError(f"Error reading S3 folder data: {str(e)}", "S3_READ_FAILED", {
                "bucket": bucket_name,
                "folder": folder_path,
                "file_type": file_type
            })

@log_function_call
def create_visualizations(df, rows_loaded_info=None):
    """Create basic data summary visualizations"""
    try:
        logger.info(f"VISUALIZATION_START: Processing {len(df):,} rows, {len(df.columns)} columns")
        
        visualizations = []
        
        # Convert all column names to uppercase for unification
        df.columns = df.columns.str.upper()
        logger.debug(f"COLUMN_STANDARDIZATION: Converted all column names to uppercase")
        
        # Get numeric and categorical columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        # Get datetime columns
        datetime_cols = df.select_dtypes(include=['datetime64', 'datetime']).columns.tolist()
        
        logger.debug(f"COLUMN_ANALYSIS: {len(numeric_cols)} numeric, {len(categorical_cols)} categorical, {len(datetime_cols)} datetime")
        
        # 1. Data preview (return raw data for frontend pagination)
        preview_df = df.head(10000).copy()
        
        # Handle NaN values properly for different column types
        for col in preview_df.columns:
            if preview_df[col].dtype.name == 'category':
                # For categorical columns, add empty string to categories first, then fill
                try:
                    if '' not in preview_df[col].cat.categories:
                        preview_df[col] = preview_df[col].cat.add_categories([''])
                    preview_df[col] = preview_df[col].fillna('')
                except Exception as e:
                    # If categorical handling fails, convert to string
                    logger.debug(f"CATEGORICAL_CONVERSION: Converting {col} to string due to: {str(e)}")
                    preview_df[col] = preview_df[col].astype(str).fillna('')
            else:
                # For non-categorical columns, fill normally
                preview_df[col] = preview_df[col].fillna('')
        
        # Format datetime columns for better display (YYYY-MM-DD format only)
        for col in datetime_cols:
            if col in preview_df.columns:
                # Format datetime to YYYY-MM-DD format
                formatted_col = preview_df[col].dt.strftime('%Y-%m-%d')
                preview_df[col] = formatted_col.fillna('')
        
        # Also check for object columns that might contain datetime strings
        for col in categorical_cols:
            if col in preview_df.columns and len(preview_df) > 0:
                # Get a sample value to check if it looks like datetime
                sample_values = preview_df[col].dropna().head(3).compute() if hasattr(preview_df[col], 'compute') else preview_df[col].dropna().head(3)
                if len(sample_values) > 0:
                    sample_val = sample_values.iloc[0]
                    if isinstance(sample_val, str) and len(sample_val) > 10:
                        # Check for common datetime patterns (GMT, UTC, or typical datetime separators)
                        if ('GMT' in sample_val or 'UTC' in sample_val or 
                            (len(sample_val) > 15 and sample_val.count(':') >= 1 and 
                             any(x in sample_val for x in ['-', '/', ' ']))):
                            try:
                                # Try to convert to datetime and format using Dask
                                temp_series = dd.to_datetime(preview_df[col], errors='coerce')
                                # Check if conversion was successful by computing a small sample
                                test_result = temp_series.dropna().head(1).compute()
                                if len(test_result) > 0:  # If conversion was successful
                                    formatted_series = temp_series.dt.strftime('%Y-%m-%d')
                                    preview_df[col] = formatted_series.fillna('')
                                    datetime_cols.append(col)  # Add to datetime columns list
                                    logger.debug(f"DATETIME_CONVERSION: Converted object column '{col}' to formatted date")
                            except Exception as e:
                                logger.debug(f"DATETIME_CONVERSION_FAILED: Could not convert column '{col}': {str(e)}")
        preview_data = {
            'columns': df.columns.tolist(),
            'data': preview_df.values.tolist(),
            'total_rows': len(df),
            'rows_loaded_info': rows_loaded_info
        }
        visualizations.append({
            'type': 'data_preview',
            'title': 'Data Preview',
            'data': preview_data
        })
        logger.debug("VISUALIZATION: Data preview created")
        
        # 2. Comprehensive data info with statistics
        logger.debug("STATISTICS: Computing comprehensive dataset statistics")
        
        # Column-level information with detailed statistics
        column_info = {}
        for col in df.columns:
            missing_count = int(df[col].isnull().sum())
            col_stats = {
                'dtype': str(df[col].dtype),
                'missing_count': missing_count,
                'missing_percentage': round((missing_count / len(df)) * 100, 2),
                'unique_count': int(df[col].nunique()),
                'unique_percentage': round((df[col].nunique() / len(df)) * 100, 2)
            }
            
            # Add type-specific statistics
            if col in numeric_cols:
                try:
                    col_stats.update({
                        'min': float(df[col].min()) if not df[col].empty else None,
                        'max': float(df[col].max()) if not df[col].empty else None,
                        'mean': float(df[col].mean()) if not df[col].empty else None,
                        'median': float(df[col].median()) if not df[col].empty else None,
                        'std': float(df[col].std()) if not df[col].empty else None,
                        'skewness': float(df[col].skew()) if not df[col].empty else None,
                        'kurtosis': float(df[col].kurtosis()) if not df[col].empty else None,
                        'q25': float(df[col].quantile(0.25)) if not df[col].empty else None,
                        'q75': float(df[col].quantile(0.75)) if not df[col].empty else None,
                        'zeros_count': int((df[col] == 0).sum()),
                        'negative_count': int((df[col] < 0).sum()),
                        'positive_count': int((df[col] > 0).sum())
                    })
                except Exception as e:
                    logger.debug(f"STATISTICS_WARNING: Could not compute numeric stats for {col}: {str(e)}")
                    
            elif col in categorical_cols or df[col].dtype == 'object':
                try:
                    # Get top values
                    value_counts = df[col].value_counts().head(5)
                    top_values = []
                    for value, count in value_counts.items():
                        top_values.append({
                            'value': str(value),
                            'count': int(count),
                            'percentage': round((count / len(df)) * 100, 2)
                        })
                    
                    col_stats.update({
                        'top_values': top_values,
                        'mode': str(df[col].mode().iloc[0]) if not df[col].mode().empty else None,
                        'avg_length': float(df[col].astype(str).str.len().mean()) if not df[col].empty else None,
                        'min_length': int(df[col].astype(str).str.len().min()) if not df[col].empty else None,
                        'max_length': int(df[col].astype(str).str.len().max()) if not df[col].empty else None
                    })
                except Exception as e:
                    logger.debug(f"STATISTICS_WARNING: Could not compute categorical stats for {col}: {str(e)}")
            
            # Check for potential data quality issues
            col_stats['data_quality'] = {}
            if missing_count > 0:
                col_stats['data_quality']['has_missing'] = True
            if col in numeric_cols and 'min' in col_stats and 'max' in col_stats:
                if col_stats['min'] == col_stats['max']:
                    col_stats['data_quality']['constant_value'] = True
            if df[col].nunique() == len(df):
                col_stats['data_quality']['all_unique'] = True
                
            column_info[col] = col_stats
            
            if missing_count > 0:
                logger.debug(f"MISSING_DATA: {col} has {missing_count:,} missing values ({col_stats['missing_percentage']:.1f}%)")
        
        # Dataset-level statistics
        total_cells = int(df.shape[0] * df.shape[1])
        missing_cells = int(df.isnull().sum().sum())
        
        # Memory usage estimation
        memory_usage_mb = float(df.memory_usage(deep=True).sum() / 1024 / 1024)
        
        # Duplicate rows analysis
        duplicate_rows = int(df.duplicated().sum())
        
        # Data completeness score
        completeness_score = round(((total_cells - missing_cells) / total_cells) * 100, 2) if total_cells > 0 else 0
        
        info_data = {
            'shape': [int(df.shape[0]), int(df.shape[1])],
            'columns': int(len(df.columns)),
            'numeric_columns': int(len(numeric_cols)),
            'categorical_columns': int(len(categorical_cols)),
            'missing_values': missing_cells,
            'missing_percentage': round((missing_cells / total_cells) * 100, 2) if total_cells > 0 else 0,
            'duplicate_rows': duplicate_rows,
            'duplicate_percentage': round((duplicate_rows / len(df)) * 100, 2) if len(df) > 0 else 0,
            'memory_usage_mb': round(memory_usage_mb, 2),
            'completeness_score': completeness_score,
            'column_info': column_info,
            'summary_stats': {
                'total_cells': total_cells,
                'unique_rows': int(len(df) - duplicate_rows),
                'avg_unique_per_column': round(sum([info['unique_count'] for info in column_info.values()]) / len(column_info), 1) if column_info else 0,
                'columns_with_missing': len([col for col, info in column_info.items() if info['missing_count'] > 0]),
                'columns_all_unique': len([col for col, info in column_info.items() if info.get('data_quality', {}).get('all_unique', False)]),
                'numeric_columns_summary': {
                    'count': len(numeric_cols),
                    'with_negatives': len([col for col in numeric_cols if column_info.get(col, {}).get('negative_count', 0) > 0]),
                    'with_zeros': len([col for col in numeric_cols if column_info.get(col, {}).get('zeros_count', 0) > 0])
                } if numeric_cols else None
            }
        }
        visualizations.append({
            'type': 'info',
            'title': 'Dataset Information',
            'data': info_data
        })
        logger.debug("VISUALIZATION: Dataset information created")
        
        logger.info(f"VISUALIZATION_SUCCESS: Created {len(visualizations)} visualizations")
        return visualizations
        
    except Exception as e:
        logger.error(f"VISUALIZATION_ERROR: {str(e)}")
        logger.error(f"VISUALIZATION_TRACEBACK: {traceback.format_exc()}")
        raise FileProcessingError(f"Error creating visualizations: {str(e)}", "VISUALIZATION_FAILED")

@app.route('/')
@log_api_request
def index():
    """Serve the main application page"""
    logger.info("PAGE_REQUEST: Main application page requested")
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
@log_api_request
def upload_file():
    """Handle file upload and processing"""
    try:
        start_time = datetime.utcnow()
        logger.info("UPLOAD_REQUEST: File upload initiated")
        
        if 'file' not in request.files:
            logger.warning("UPLOAD_ERROR: No file in request")
            raise ValidationError('No file selected', 'NO_FILE')
        
        file = request.files['file']
        if file.filename == '':
            logger.warning("UPLOAD_ERROR: Empty filename")
            raise ValidationError('No file selected', 'EMPTY_FILENAME')
        
        if not file or not allowed_file(file.filename):
            logger.warning(f"UPLOAD_ERROR: Invalid file type: {file.filename}")
            raise ValidationError('Invalid file type. Only CSV and Parquet files are allowed.', 'INVALID_FILE_TYPE')
        
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        logger.info(f"UPLOAD_SAVE: Saving file to {file_path}")
        file.save(file_path)
        
        # Get rows limit parameter
        rows_limit = request.form.get('rows_limit', 'all')
        logger.info(f"UPLOAD_PARAMS: rows_limit={rows_limit}")
        
        # Determine file type
        file_type = filename.rsplit('.', 1)[1].lower()
        
        # Read and process the data using Dask
        df = read_file_data_dask(file_path, file_type, rows_limit)
        
        # Create rows loaded info message
        rows_loaded_info = None
        if rows_limit != 'all':
            rows_loaded_info = f"Loaded first {len(df):,} rows (limited by user selection)"
        
        visualizations = create_visualizations(df, rows_loaded_info)
        
        # Calculate total processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()
        
        # Clean up uploaded file
        logger.info(f"UPLOAD_CLEANUP: Removing temporary file {file_path}")
        os.remove(file_path)
        
        logger.info(f"UPLOAD_SUCCESS: File {filename} processed successfully in {processing_time:.2f}s")
        return jsonify({
            'success': True,
            'visualizations': visualizations,
            'filename': filename,
            'processing_time': round(processing_time, 2),
            'export_available': True
        })
        
    except DataVisualizerError as e:
        logger.error(f"UPLOAD_BUSINESS_ERROR: {e.error_code} - {str(e)}")
        return jsonify({
            'error': str(e),
            'error_code': e.error_code,
            'timestamp': e.timestamp
        }), 400
        
    except Exception as e:
        logger.error(f"UPLOAD_SYSTEM_ERROR: Unexpected error - {str(e)}")
        logger.error(f"UPLOAD_SYSTEM_TRACEBACK: {traceback.format_exc()}")
        return jsonify({
            'error': 'An unexpected error occurred during file processing',
            'error_code': 'SYSTEM_ERROR',
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/s3-data', methods=['POST'])
@log_api_request
def load_s3_data():
    """Handle S3 data loading"""
    try:
        start_time = datetime.utcnow()
        logger.info("S3_REQUEST: S3 data loading initiated")
        
        data = request.get_json()
        s3_url = data.get('s3_url')
        file_type = data.get('file_type', 'csv')
        sample_size = data.get('sample_size', 50000)
        
        logger.info(f"S3_PARAMS: url={s3_url}, type={file_type}, sample={sample_size}")
        
        if not s3_url:
            logger.warning("S3_ERROR: Missing S3 URL")
            raise ValidationError('S3 URL is required', 'MISSING_S3_URL')
        
        if file_type not in ALLOWED_EXTENSIONS:
            logger.warning(f"S3_ERROR: Unsupported file type: {file_type}")
            raise ValidationError(f'Unsupported file type: {file_type}', 'UNSUPPORTED_FILE_TYPE')
        
        # Parse S3 URL to extract bucket and object path, and determine if it's a file or folder
        bucket_name, object_path, is_file = parse_s3_url(s3_url)
        
        # Read data from S3 using appropriate method based on whether it's a file or folder
        if is_file:
            logger.info(f"S3_PROCESSING: Individual file detected - {object_path}")
            df, rows_loaded_info = read_s3_file_dask(bucket_name, object_path, file_type, sample_size)
        else:
            logger.info(f"S3_PROCESSING: Folder/directory detected - {object_path}")
            df, rows_loaded_info = read_s3_data_dask(bucket_name, object_path, file_type, sample_size)
        visualizations = create_visualizations(df, rows_loaded_info)
        
        # Calculate total processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()
        
        dataset_name = object_path.rstrip('/').split('/')[-1] or bucket_name
        logger.info(f"S3_SUCCESS: Dataset {dataset_name} processed successfully in {processing_time:.2f}s")
        
        return jsonify({
            'success': True,
            'visualizations': visualizations,
            'filename': dataset_name,
            'processing_time': round(processing_time, 2),
            'export_available': True
        })
        
    except NoCredentialsError:
        logger.error("S3_AUTH_ERROR: AWS credentials not found")
        return jsonify({
            'error': 'AWS credentials not found. Please configure your AWS credentials.',
            'error_code': 'NO_CREDENTIALS',
            'timestamp': datetime.utcnow().isoformat()
        }), 401
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"S3_CLIENT_ERROR: AWS ClientError - {error_code} - {str(e)}")
        return jsonify({
            'error': f'AWS error: {str(e)}',
            'error_code': f'AWS_{error_code}',
            'timestamp': datetime.utcnow().isoformat()
        }), 400
        
    except DataVisualizerError as e:
        logger.error(f"S3_BUSINESS_ERROR: {e.error_code} - {str(e)}")
        return jsonify({
            'error': str(e),
            'error_code': e.error_code,
            'timestamp': e.timestamp
        }), 400
        
    except Exception as e:
        logger.error(f"S3_SYSTEM_ERROR: Unexpected error - {str(e)}")
        logger.error(f"S3_SYSTEM_TRACEBACK: {traceback.format_exc()}")
        return jsonify({
            'error': 'An unexpected error occurred during S3 data processing',
            'error_code': 'SYSTEM_ERROR',
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/export/<format_type>')
@log_api_request
def export_data(format_type):
    """Export currently loaded data in specified format"""
    try:
        logger.info(f"EXPORT_REQUEST: Export data as {format_type}")
        
        # Validate format
        if format_type not in ['csv', 'parquet']:
            logger.warning(f"EXPORT_ERROR: Invalid format: {format_type}")
            raise ValidationError(f'Invalid export format: {format_type}', 'INVALID_FORMAT')
        
        # Check if we have data stored in session or request
        # For now, we'll return an error since we need to implement data persistence
        # In a production app, you'd store the DataFrame in session or cache
        logger.warning("EXPORT_ERROR: No data available for export")
        return jsonify({
            'error': 'No data available for export. Please load data first.',
            'error_code': 'NO_DATA_AVAILABLE',
            'timestamp': datetime.utcnow().isoformat()
        }), 400
        
    except DataVisualizerError as e:
        logger.error(f"EXPORT_BUSINESS_ERROR: {e.error_code} - {str(e)}")
        return jsonify({
            'error': str(e),
            'error_code': e.error_code,
            'timestamp': e.timestamp
        }), 400
        
    except Exception as e:
        logger.error(f"EXPORT_SYSTEM_ERROR: Unexpected error - {str(e)}")
        logger.error(f"EXPORT_SYSTEM_TRACEBACK: {traceback.format_exc()}")
        return jsonify({
            'error': 'An unexpected error occurred during export',
            'error_code': 'SYSTEM_ERROR',
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/export-s3/<format_type>', methods=['POST'])
@log_api_request
def export_s3_data(format_type):
    """Re-load and export S3 data in specified format"""
    try:
        start_time = datetime.utcnow()
        logger.info(f"EXPORT_S3_REQUEST: Export S3 data as {format_type}")
        
        # Validate format
        if format_type not in ['csv', 'parquet']:
            logger.warning(f"EXPORT_ERROR: Invalid format: {format_type}")
            raise ValidationError(f'Invalid export format: {format_type}', 'INVALID_FORMAT')
        
        data = request.get_json()
        s3_url = data.get('s3_url')
        file_type = data.get('file_type', 'csv')
        sample_size = data.get('sample_size', 50000)
        
        logger.info(f"EXPORT_S3_PARAMS: url={s3_url}, type={file_type}, sample={sample_size}")
        
        if not s3_url:
            logger.warning("EXPORT_ERROR: Missing S3 URL")
            raise ValidationError('S3 URL is required', 'MISSING_S3_URL')
        
        # Parse S3 URL to extract bucket and object path, and determine if it's a file or folder
        bucket_name, object_path, is_file = parse_s3_url(s3_url)
        
        # Read data from S3 using appropriate method based on whether it's a file or folder
        if is_file:
            logger.info(f"EXPORT_S3_PROCESSING: Individual file detected - {object_path}")
            df, _ = read_s3_file_dask(bucket_name, object_path, file_type, sample_size)
        else:
            logger.info(f"EXPORT_S3_PROCESSING: Folder/directory detected - {object_path}")
            df, _ = read_s3_data_dask(bucket_name, object_path, file_type, sample_size)
        
        # Generate export filename
        dataset_name = object_path.rstrip('/').split('/')[-1] or bucket_name
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        export_filename = f"{dataset_name}_export_{timestamp}.{format_type}"
        export_path = os.path.join(app.config['UPLOAD_FOLDER'], export_filename)
        
        # Export data in requested format
        if format_type == 'csv':
            df.to_csv(export_path, index=False)
            logger.info(f"EXPORT_SUCCESS: CSV exported to {export_path}")
        elif format_type == 'parquet':
            if not PARQUET_AVAILABLE:
                raise ValidationError('Parquet export not available. PyArrow not installed.', 'PARQUET_NOT_AVAILABLE')
            df.to_parquet(export_path, index=False)
            logger.info(f"EXPORT_SUCCESS: Parquet exported to {export_path}")
        
        # Verify file was created and get size
        if not os.path.exists(export_path):
            raise FileProcessingError(f"Export file was not created: {export_path}", "FILE_NOT_CREATED")
        
        file_size = os.path.getsize(export_path)
        logger.info(f"EXPORT_FILE_INFO: {export_filename} created, size: {file_size} bytes")
        
        # Calculate processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()
        
        logger.info(f"EXPORT_S3_SUCCESS: {export_filename} created in {processing_time:.2f}s")
        
        # Clean up file after sending (optional - comment out for debugging)
        def cleanup_file():
            try:
                if os.path.exists(export_path):
                    os.remove(export_path)
                    logger.info(f"EXPORT_CLEANUP: Removed {export_path}")
            except Exception as e:
                logger.warning(f"EXPORT_CLEANUP_ERROR: {str(e)}")
        
        # Schedule cleanup after response is sent
        import threading
        cleanup_timer = threading.Timer(5.0, cleanup_file)
        cleanup_timer.start()
        
        return send_from_directory(
            app.config['UPLOAD_FOLDER'], 
            export_filename, 
            as_attachment=True,
            download_name=export_filename
        )
        
    except DataVisualizerError as e:
        logger.error(f"EXPORT_S3_BUSINESS_ERROR: {e.error_code} - {str(e)}")
        return jsonify({
            'error': str(e),
            'error_code': e.error_code,
            'timestamp': e.timestamp
        }), 400
        
    except Exception as e:
        logger.error(f"EXPORT_S3_SYSTEM_ERROR: Unexpected error - {str(e)}")
        logger.error(f"EXPORT_S3_SYSTEM_TRACEBACK: {traceback.format_exc()}")
        return jsonify({
            'error': 'An unexpected error occurred during export',
            'error_code': 'SYSTEM_ERROR',
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/aws-status')
@log_api_request
def aws_status():
    """Check AWS connectivity status"""
    try:
        # logger.info("AWS_STATUS_CHECK: Checking AWS connectivity")
        
        # Try to import boto3 and check credentials
        try:
            import boto3
            from botocore.exceptions import NoCredentialsError, ClientError
            
            # Create a simple S3 client to test connectivity
            s3_client = boto3.client('s3')
            
            # Try to list buckets (this requires minimal permissions)
            try:
                s3_client.list_buckets()
                aws_status = {
                    'connected': True,
                    'status': 'Connected to AWS services',
                    'details': 'AWS credentials are configured and accessible'
                }
                # logger.info("AWS_STATUS_CHECK: Successfully connected to AWS")
            except NoCredentialsError:
                aws_status = {
                    'connected': False,
                    'status': 'AWS credentials not found',
                    'details': 'Please configure your AWS credentials'
                }
                logger.warning("AWS_STATUS_CHECK: No AWS credentials found")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'AccessDenied':
                    aws_status = {
                        'connected': True,
                        'status': 'Connected with limited permissions',
                        'details': 'AWS credentials are configured but may have limited S3 access'
                    }
                    logger.info("AWS_STATUS_CHECK: Connected to AWS with limited permissions")
                else:
                    aws_status = {
                        'connected': False,
                        'status': f'AWS connection error: {error_code}',
                        'details': str(e)
                    }
                    logger.warning(f"AWS_STATUS_CHECK: AWS connection error - {error_code}")
            except Exception as e:
                aws_status = {
                    'connected': False,
                    'status': 'AWS connection failed',
                    'details': str(e)
                }
                logger.warning(f"AWS_STATUS_CHECK: Unexpected AWS error - {str(e)}")
                
        except ImportError:
            aws_status = {
                'connected': False,
                'status': 'AWS SDK not available',
                'details': 'boto3 library is not installed'
            }
            logger.warning("AWS_STATUS_CHECK: boto3 not available")
        
        return jsonify({
            'timestamp': datetime.utcnow().isoformat(),
            'aws': aws_status
        })
        
    except Exception as e:
        logger.error(f"AWS_STATUS_CHECK_ERROR: {str(e)}")
        return jsonify({
            'timestamp': datetime.utcnow().isoformat(),
            'aws': {
                'connected': False,
                'status': 'Status check failed',
                'details': str(e)
            }
        }), 500

@app.route('/system-stats')
@log_api_request
def system_stats():
    """Get real-time system statistics (RAM and CPU usage)"""
    try:
        logger.debug("SYSTEM_STATS: Collecting system statistics")
        
        # Get memory information
        memory = psutil.virtual_memory()
        memory_stats = {
            'total_gb': round(memory.total / (1024**3), 2),
            'available_gb': round(memory.available / (1024**3), 2),
            'used_gb': round(memory.used / (1024**3), 2),
            'percent_used': round(memory.percent, 1),
            'percent_available': round((memory.available / memory.total) * 100, 1)
        }
        
        # Get CPU information
        cpu_percent = psutil.cpu_percent(interval=0.1)  # Short interval for responsiveness
        cpu_stats = {
            'percent_used': round(cpu_percent, 1),
            'core_count': psutil.cpu_count(),
            'core_count_logical': psutil.cpu_count(logical=True)
        }
        
        system_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'memory': memory_stats,
            'cpu': cpu_stats
        }
        
        logger.debug(f"SYSTEM_STATS: RAM: {memory_stats['percent_used']}%, CPU: {cpu_stats['percent_used']}%")
        return jsonify(system_info)
        
    except Exception as e:
        logger.error(f"SYSTEM_STATS_ERROR: {str(e)}")
        return jsonify({
            'error': 'Failed to collect system statistics',
            'error_code': 'SYSTEM_STATS_ERROR',
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/health')
@log_api_request
def health_check():
    """Health check endpoint"""
    logger.debug("HEALTH_CHECK: Application health requested")
    
    health_info = {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'features': {
            'parquet_support': PARQUET_AVAILABLE,
            's3_support': S3FS_AVAILABLE
        }
    }
    
    logger.debug(f"HEALTH_CHECK: {health_info}")
    return jsonify(health_info)

# Global error handlers
@app.errorhandler(404)
def not_found_error(error):
    logger.warning(f"HTTP_404: {request.method} {request.url} - Page not found")
    return jsonify({
        'error': 'Page not found',
        'error_code': 'NOT_FOUND',
        'timestamp': datetime.utcnow().isoformat()
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"HTTP_500: Internal server error - {str(error)}")
    return jsonify({
        'error': 'Internal server error',
        'error_code': 'INTERNAL_ERROR',
        'timestamp': datetime.utcnow().isoformat()
    }), 500

# Log application ready
logger.info("APPLICATION_READY: Data Visualizer application initialized successfully")

if __name__ == '__main__':
    logger.info("APPLICATION_START: Starting Flask development server")
    app.run(debug=True, host='0.0.0.0', port=5000) 