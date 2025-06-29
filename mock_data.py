#!/usr/bin/env python3
"""
Highly Optimized Mock Data Generator with pandas
Generates realistic fake data using vectorized operations, chunked processing, and parallel I/O.
"""

import pandas as pd
from faker import Faker
import random
import boto3
import os
import numpy as np
from botocore.exceptions import ClientError, NoCredentialsError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, timedelta

def upload_to_s3(file_path, bucket_name, s3_key):
    """Upload a file to S3 bucket"""
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except NoCredentialsError:
        print(f"AWS credentials not found. Skipping S3 upload for {file_path}")
        return False
    except ClientError as e:
        print(f"Error uploading {file_path} to S3: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error uploading {file_path}: {e}")
        return False

def generate_chunk_data(start_idx, chunk_size, thread_id):
    """Generate a chunk of data efficiently using vectorized operations"""
    # Create thread-local Faker instance with unique seed for reproducibility
    thread_fake = Faker('en_US')
    thread_fake.seed_instance(thread_id * 10000 + start_idx)
    
    # Pre-generate choice arrays for better performance
    genders = np.random.choice(['Male', 'Female', 'Other'], chunk_size)
    marital_statuses = np.random.choice(['Single', 'Married', 'Divorced', 'Widowed'], chunk_size)
    address_types = np.random.choice(['Home', 'Work', 'Billing', 'Shipping'], chunk_size)
    regions = np.random.choice(['North', 'South', 'East', 'West', 'Central'], chunk_size)
    departments = np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations'], chunk_size)
    employment_statuses = np.random.choice(['Full-time', 'Part-time', 'Contract', 'Unemployed', 'Retired'], chunk_size)
    education_levels = np.random.choice(['High School', 'Bachelor', 'Master', 'PhD', 'Associate'], chunk_size)
    industries = np.random.choice(['Technology', 'Healthcare', 'Finance', 'Education', 'Retail', 'Manufacturing'], chunk_size)
    work_locations = np.random.choice(['Office', 'Remote', 'Hybrid'], chunk_size)
    contact_methods = np.random.choice(['Email', 'Phone', 'SMS', 'Mail'], chunk_size)
    subscription_tiers = np.random.choice(['Basic', 'Premium', 'Enterprise', 'Free'], chunk_size)
    referral_sources = np.random.choice(['Google', 'Facebook', 'Friend', 'Advertisement', 'Direct'], chunk_size)
    risk_categories = np.random.choice(['Low', 'Medium', 'High'], chunk_size)
    currencies = np.random.choice(['USD', 'EUR', 'GBP', 'CAD', 'AUD'], chunk_size)
    
    # Generate vectorized data
    chunk_data = {
        'customer_id': np.arange(start_idx + 1, start_idx + chunk_size + 1),
        'first_name': [thread_fake.first_name() for _ in range(chunk_size)],
        'last_name': [thread_fake.last_name() for _ in range(chunk_size)],
        'full_name': [thread_fake.name() for _ in range(chunk_size)],
        'email_address': [thread_fake.email() for _ in range(chunk_size)],
        'phone_number': [thread_fake.phone_number() for _ in range(chunk_size)],
        'ssn': [thread_fake.ssn() for _ in range(chunk_size)],
        'date_of_birth': [thread_fake.date_of_birth(minimum_age=18, maximum_age=85) for _ in range(chunk_size)],
        'gender': genders,
        'marital_status': marital_statuses,
        'street_address': [thread_fake.street_address() for _ in range(chunk_size)],
        'city': [thread_fake.city() for _ in range(chunk_size)],
        'state': [thread_fake.state() for _ in range(chunk_size)],
        'zip_code': [thread_fake.zipcode() for _ in range(chunk_size)],
        'country': [thread_fake.country() for _ in range(chunk_size)],
        'latitude': np.random.uniform(-90, 90, chunk_size),
        'longitude': np.random.uniform(-180, 180, chunk_size),
        'time_zone': [thread_fake.timezone() for _ in range(chunk_size)],
        'address_type': address_types,
        'region': regions,
        'annual_salary': np.random.randint(30000, 250001, chunk_size),
        'credit_score': np.random.randint(300, 851, chunk_size),
        'bank_account_number': [thread_fake.bban() for _ in range(chunk_size)],
        'credit_card_number': [thread_fake.credit_card_number() for _ in range(chunk_size)],
        'credit_card_provider': [thread_fake.credit_card_provider() for _ in range(chunk_size)],
        'iban': [thread_fake.iban() for _ in range(chunk_size)],
        'currency_code': currencies,
        'account_balance': np.round(np.random.uniform(-5000, 50000, chunk_size), 2),
        'monthly_expenses': np.random.randint(1500, 8001, chunk_size),
        'investment_portfolio_value': np.round(np.random.uniform(0, 500000, chunk_size), 2),
        'job_title': [thread_fake.job() for _ in range(chunk_size)],
        'company_name': [thread_fake.company() for _ in range(chunk_size)],
        'department': departments,
        'employment_status': employment_statuses,
        'years_experience': np.random.randint(0, 41, chunk_size),
        'education_level': education_levels,
        'industry': industries,
        'work_location': work_locations,
        'manager_id': np.where(np.random.random(chunk_size) > 0.1, np.random.randint(1, 1001, chunk_size), None),
        'hire_date': [thread_fake.date_between(start_date='-10y', end_date='today') for _ in range(chunk_size)],
        'is_active': np.random.choice([True, False], chunk_size),
        'preferred_contact_method': contact_methods,
        'subscription_tier': subscription_tiers,
        'last_login_date': [thread_fake.date_time_between(start_date='-1y', end_date='now') for _ in range(chunk_size)],
        'total_purchases': np.random.randint(0, 501, chunk_size),
        'total_spent': np.round(np.random.uniform(0, 25000, chunk_size), 2),
        'customer_satisfaction_score': np.random.randint(1, 11, chunk_size),
        'referral_source': referral_sources,
        'loyalty_points': np.random.randint(0, 10001, chunk_size),
        'risk_category': risk_categories
    }
    
    return pd.DataFrame(chunk_data)

def main():
    print("Generating mock data with EXTREME OPTIMIZATION...")
    
    # Configuration
    num_rows = 10_000
    bucket_name = "aws-bucket-3773220589"
    csv_folder = "data_analytics_platform/csv/year=2025/month=06/day=29"
    parquet_folder = "data_analytics_platform/prq/year=2025/month=06/day=29"
    max_workers = os.cpu_count()
    
    # Optimize chunk size based on available cores and total rows
    optimal_chunk_size = max(100, min(10000, num_rows // max_workers))
    if num_rows < max_workers:
        optimal_chunk_size = max(1, num_rows // max_workers) if num_rows > max_workers else 1
        max_workers = min(max_workers, num_rows)
    
    print(f"Configuration:")
    print(f"   - Total rows: {num_rows:,}")
    print(f"   - CPU cores available: {os.cpu_count()}")
    print(f"   - Workers used: {max_workers}")
    print(f"   - Optimal chunk size: {optimal_chunk_size:,}")
    print(f"   - Estimated chunks: {(num_rows + optimal_chunk_size - 1) // optimal_chunk_size}")
    
    start_time = datetime.now()
    
    # Calculate chunk ranges
    chunk_ranges = []
    for i in range(0, num_rows, optimal_chunk_size):
        actual_chunk_size = min(optimal_chunk_size, num_rows - i)
        chunk_ranges.append((i, actual_chunk_size))
    
    print(f"\nGenerating {len(chunk_ranges)} chunks in parallel...")
    
    # Generate chunks in parallel with progress tracking
    chunks = [None] * len(chunk_ranges)  # Pre-allocate for order preservation
    completed_chunks = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit chunk generation tasks
        future_to_index = {
            executor.submit(generate_chunk_data, start_idx, size, idx): idx 
            for idx, (start_idx, size) in enumerate(chunk_ranges)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_index):
            chunk_idx = future_to_index[future]
            chunks[chunk_idx] = future.result()
            completed_chunks += 1
            
            # Progress reporting
            progress_percentage = (completed_chunks / len(chunk_ranges)) * 100
            current_time = datetime.now()
            elapsed_time = (current_time - start_time).total_seconds()
            
            if elapsed_time > 0 and completed_chunks > 0:
                estimated_total_time = elapsed_time / (progress_percentage / 100)
                estimated_remaining_time = max(0, estimated_total_time - elapsed_time)
                estimated_remaining_minutes = estimated_remaining_time / 60
                
                print(f"   Chunk {completed_chunks}/{len(chunk_ranges)} "
                      f"({progress_percentage:.1f}% complete, "
                      f"{estimated_remaining_minutes:.1f}min remaining)")
            else:
                print(f"   Chunk {completed_chunks}/{len(chunk_ranges)} ({progress_percentage:.1f}% complete)")
    
    # Combine all chunks efficiently
    print("\nCombining chunks into final DataFrame...")
    combine_start = datetime.now()
    df = pd.concat(chunks, ignore_index=True)
    combine_time = (datetime.now() - combine_start).total_seconds()
    
    generation_time = (datetime.now() - start_time).total_seconds()
    print(f"Data generation completed: {len(df):,} rows × {len(df.columns)} columns")
    print(f"   Generation time: {generation_time:.2f}s")
    print(f"   Combine time: {combine_time:.2f}s")
    print(f"   Rate: {len(df) / generation_time:,.0f} rows/second")
    
    # Save files in parallel
    print("\nSaving files in parallel...")
    save_start_time = datetime.now()
    
    def save_csv():
        df.to_csv('sample.csv', index=False)
        return 'CSV'
    
    def save_parquet():
        df.to_parquet('sample.parquet', index=False)
        return 'Parquet'
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        save_futures = [executor.submit(save_csv), executor.submit(save_parquet)]
        
        for future in as_completed(save_futures):
            file_type = future.result()
            print(f"   {file_type} saved")
    
    save_time = (datetime.now() - save_start_time).total_seconds()
    total_time = (datetime.now() - start_time).total_seconds()
    
    print(f"File saving completed in {save_time:.2f}s")
    print(f"TOTAL PROCESS TIME: {total_time:.2f}s")
    
    # Show results
    print(f"\nResults Summary:")
    print(f"   - DataFrame shape: {df.shape}")
    print(f"   - Columns: {len(df.columns)}")
    print(f"   - Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    
    print(f"\nFile Information:")
    csv_size = os.path.getsize('sample.csv')
    parquet_size = os.path.getsize('sample.parquet')
    print(f"   - CSV: {csv_size:,} bytes ({csv_size/1024/1024:.1f} MB)")
    print(f"   - Parquet: {parquet_size:,} bytes ({parquet_size/1024/1024:.1f} MB)")
    print(f"   - Compression ratio: {csv_size/parquet_size:.1f}x")
    
    # Sample data preview
    print(f"\nSample Data (first 3 rows, first 10 columns):")
    print(df.iloc[:3, :10].to_string())
    
    # Upload to S3 in parallel
    print(f"\nUploading to S3 bucket: {bucket_name}")
    upload_start = datetime.now()
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        upload_futures = [
            executor.submit(upload_to_s3, 'sample.csv', bucket_name, f"{csv_folder}/sample.csv"),
            executor.submit(upload_to_s3, 'sample.parquet', bucket_name, f"{parquet_folder}/sample.parquet")
        ]
        
        csv_uploaded, parquet_uploaded = [future.result() for future in upload_futures]
    
    upload_time = (datetime.now() - upload_start).total_seconds()
    
    print(f"\nPROCESS COMPLETED!")
    print(f"   Total time: {total_time:.2f}s")
    print(f"   Performance: {len(df) / total_time:,.0f} rows/second")
    print(f"   Local files: sample.csv, sample.parquet")
    
    if csv_uploaded:
        print(f"   CSV: s3://{bucket_name}/{csv_folder}/sample.csv")
    if parquet_uploaded:
        print(f"   Parquet: s3://{bucket_name}/{parquet_folder}/sample.parquet")

if __name__ == '__main__':
    main()
