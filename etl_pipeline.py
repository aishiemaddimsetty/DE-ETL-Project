"""
ETL Pipeline using PySpark for processing shopper events
Transforms raw events into analytics-ready format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3

class ShopperETLPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ShopperAnalyticsETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def extract_from_s3(self, s3_path: str):
        """Extract raw events from S3"""
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("device_type", StringType(), True),
            StructField("ad_campaign_id", StringType(), True),
            StructField("revenue", DoubleType(), True)
        ])
        
        df = self.spark.read.json(s3_path, schema=schema)
        return df
    
    def transform_events(self, raw_df):
        """Transform raw events for analytics"""
        
        # Add derived columns
        transformed_df = raw_df \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0)) \
            .withColumn("is_ad_driven", when(col("ad_campaign_id").isNotNull(), 1).otherwise(0))
        
        # Create user session metrics
        session_metrics = transformed_df.groupBy("user_id", "session_id", "date") \
            .agg(
                count("*").alias("total_events"),
                sum("is_purchase").alias("purchases"),
                sum("revenue").alias("session_revenue"),
                countDistinct("product_id").alias("unique_products_viewed"),
                first("device_type").alias("device_type")
            )
        
        # Create daily product metrics
        product_metrics = transformed_df.groupBy("product_id", "category", "date") \
            .agg(
                count("*").alias("total_views"),
                sum("is_purchase").alias("total_purchases"),
                sum("revenue").alias("total_revenue"),
                avg("price").alias("avg_price")
            ) \
            .withColumn("conversion_rate", 
                       when(col("total_views") > 0, col("total_purchases") / col("total_views"))
                       .otherwise(0))
        
        return {
            'events': transformed_df,
            'session_metrics': session_metrics,
            'product_metrics': product_metrics
        }
    
    def load_to_redshift(self, dataframes: dict, redshift_config: dict):
        """Load transformed data to Redshift"""
        
        for table_name, df in dataframes.items():
            df.write \
                .format("jdbc") \
                .option("url", redshift_config['url']) \
                .option("dbtable", f"analytics.{table_name}") \
                .option("user", redshift_config['user']) \
                .option("password", redshift_config['password']) \
                .option("driver", "com.amazon.redshift.jdbc.Driver") \
                .mode("append") \
                .save()
            
            print(f"Loaded {df.count()} records to {table_name}")
    
    def run_etl(self, s3_input_path: str, redshift_config: dict):
        """Execute complete ETL pipeline"""
        
        # Extract
        print("Extracting data from S3...")
        raw_df = self.extract_from_s3(s3_input_path)
        
        # Transform
        print("Transforming data...")
        transformed_data = self.transform_events(raw_df)
        
        # Data quality checks
        self.validate_data_quality(transformed_data['events'])
        
        # Load
        print("Loading to Redshift...")
        self.load_to_redshift(transformed_data, redshift_config)
        
        print("ETL pipeline completed successfully!")
    
    def validate_data_quality(self, df):
        """Basic data quality validation"""
        total_records = df.count()
        null_user_ids = df.filter(col("user_id").isNull()).count()
        
        if null_user_ids > 0:
            print(f"Warning: {null_user_ids} records with null user_id")
        
        if total_records == 0:
            raise ValueError("No records found in dataset")
        
        print(f"Data quality check passed: {total_records} records processed")

if __name__ == "__main__":
    etl = ShopperETLPipeline()
    
    # Configuration
    redshift_config = {
        'url': 'jdbc:redshift://your-cluster.redshift.amazonaws.com:5439/analytics',
        'user': 'your_user',
        'password': 'your_password'
    }
    
    # Run ETL
    etl.run_etl('s3://your-bucket/raw-events/', redshift_config)