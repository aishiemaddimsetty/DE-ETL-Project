"""
Data Quality Monitoring and Validation
Implements data quality checks using Great Expectations
"""

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
import pandas as pd
import boto3
from typing import Dict, List

class DataQualityMonitor:
    def __init__(self, s3_bucket: str):
        self.context = gx.get_context()
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
    
    def create_expectations_suite(self):
        """Create data quality expectations for shopper events"""
        
        suite = self.context.create_expectation_suite(
            expectation_suite_name="shopper_events_suite",
            overwrite_existing=True
        )
        
        # Core data expectations
        expectations = [
            # Required fields should not be null
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "event_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "user_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "timestamp"}},
            
            # Event types should be valid
            {"expectation_type": "expect_column_values_to_be_in_set", 
             "kwargs": {"column": "event_type", "value_set": ["page_view", "add_to_cart", "purchase", "search", "ad_click"]}},
            
            # Price should be positive
            {"expectation_type": "expect_column_values_to_be_between", 
             "kwargs": {"column": "price", "min_value": 0, "max_value": 10000}},
            
            # Revenue should be non-negative
            {"expectation_type": "expect_column_values_to_be_between", 
             "kwargs": {"column": "revenue", "min_value": 0}},
            
            # Device type validation
            {"expectation_type": "expect_column_values_to_be_in_set", 
             "kwargs": {"column": "device_type", "value_set": ["mobile", "desktop", "tablet"]}},
            
            # Data freshness - events should be recent
            {"expectation_type": "expect_column_values_to_be_between", 
             "kwargs": {"column": "timestamp", "min_value": "2024-01-01", "parse_strings_as_datetimes": True}},
        ]
        
        for expectation in expectations:
            suite.add_expectation(**expectation)
        
        self.context.save_expectation_suite(suite)
        return suite
    
    def validate_batch(self, df: pd.DataFrame) -> Dict:
        """Validate a batch of data against expectations"""
        
        # Create datasource and asset
        datasource = self.context.sources.add_pandas("pandas_datasource")
        data_asset = datasource.add_dataframe_asset(name="shopper_events")
        
        # Create batch request
        batch_request = data_asset.build_batch_request(dataframe=df)
        
        # Create validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="shopper_events_suite"
        )
        
        # Run validation
        results = validator.validate()
        
        return {
            'success': results.success,
            'statistics': results.statistics,
            'failed_expectations': [
                exp for exp in results.results 
                if not exp.success
            ]
        }
    
    def generate_data_docs(self):
        """Generate data documentation"""
        self.context.build_data_docs()
        return "Data documentation generated successfully"
    
    def monitor_data_drift(self, current_df: pd.DataFrame, reference_df: pd.DataFrame):
        """Monitor for data drift between datasets"""
        
        drift_metrics = {}
        
        # Compare basic statistics
        for column in ['price', 'revenue']:
            if column in current_df.columns and column in reference_df.columns:
                current_mean = current_df[column].mean()
                reference_mean = reference_df[column].mean()
                
                drift_percentage = abs(current_mean - reference_mean) / reference_mean * 100
                drift_metrics[f'{column}_drift_pct'] = drift_percentage
        
        # Compare categorical distributions
        for column in ['event_type', 'device_type', 'category']:
            if column in current_df.columns and column in reference_df.columns:
                current_dist = current_df[column].value_counts(normalize=True)
                reference_dist = reference_df[column].value_counts(normalize=True)
                
                # Calculate KL divergence (simplified)
                common_values = set(current_dist.index) & set(reference_dist.index)
                if common_values:
                    kl_div = sum([
                        current_dist[val] * np.log(current_dist[val] / reference_dist[val])
                        for val in common_values
                        if reference_dist[val] > 0
                    ])
                    drift_metrics[f'{column}_kl_divergence'] = kl_div
        
        return drift_metrics

def run_quality_checks(s3_bucket: str, s3_key: str):
    """Main function to run data quality checks"""
    
    monitor = DataQualityMonitor(s3_bucket)
    
    # Create expectations suite
    monitor.create_expectations_suite()
    
    # Load data from S3
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    df = pd.read_json(obj['Body'])
    
    # Run validation
    results = monitor.validate_batch(df)
    
    # Print results
    print(f"Validation Success: {results['success']}")
    print(f"Total Expectations: {results['statistics']['evaluated_expectations']}")
    print(f"Failed Expectations: {len(results['failed_expectations'])}")
    
    if not results['success']:
        print("\nFailed Expectations:")
        for failure in results['failed_expectations']:
            print(f"- {failure['expectation_config']['expectation_type']}")
    
    return results

if __name__ == "__main__":
    # Example usage
    results = run_quality_checks('your-bucket', 'sample_events.json')