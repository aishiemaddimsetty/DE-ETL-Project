# Shopper Behavior Analytics Pipeline

A comprehensive data engineering project demonstrating real-time e-commerce analytics using AWS big data technologies.

## 🎯 Project Overview

This project simulates a production-grade data pipeline for analyzing shopper behavior and advertising effectiveness, similar to systems used at major e-commerce platforms. It showcases end-to-end data engineering skills from ingestion to insights.

## 🏗️ Architecture

```
Data Sources → Kinesis → Lambda → S3 → Glue ETL → Redshift → Analytics Dashboard
```

## 🛠️ Technologies Used

- **AWS Services**: S3, Kinesis, Lambda, Glue, Redshift, IAM, CloudWatch
- **Big Data**: Apache Spark (via AWS Glue), Parquet format
- **Languages**: Python, SQL, HiveQL
- **Data Modeling**: Star schema, dimensional modeling
- **Automation**: CloudFormation, scheduled ETL jobs

## 📊 Key Features

1. **Real-time Data Ingestion**: Simulates shopper events (page views, purchases, cart actions)
2. **Scalable ETL Pipeline**: Processes millions of events using Spark
3. **Data Warehouse**: Optimized star schema for analytics
4. **Automated Reporting**: Self-service analytics capabilities
5. **Data Quality**: Validation and monitoring throughout pipeline

## 🚀 Quick Start

1. Deploy infrastructure: `aws cloudformation deploy --template-file infrastructure.yaml`
2. Run data generator: `python src/data_generator.py`
3. Execute ETL: `python src/etl_pipeline.py`
4. Query analytics: `python src/analytics_queries.py`

## 📈 Business Impact

- **Customer Insights**: Track shopping patterns and preferences
- **Ad Optimization**: Measure advertising campaign effectiveness
- **Revenue Analytics**: Identify high-value customer segments
- **Real-time Monitoring**: Track KPIs and system health

## 🎯  Talking Points

- Designed for 10M+ daily events with sub-second latency
- Implements data quality checks and error handling
- Cost-optimized using S3 lifecycle policies and Redshift compression
- Demonstrates understanding of CAP theorem and eventual consistency
- Shows experience with both batch and streaming processing