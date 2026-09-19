# EV Charging Data Engineering & Real-Time Analytics Platform

An end-to-end data engineering platform for processing historical and real-time electric vehicle (EV) charging data.

This project demonstrates batch processing, real-time streaming, data quality, medallion architecture, analytics, and cloud data engineering using PySpark, Apache Kafka, and Azure services.

## Project Objective

Build a scalable data platform that processes EV charging data and provides analytics on:

- Energy consumption
- Charging revenue
- Station utilization
- Customer charging behavior
- Vehicle charging behavior
- Charging demand

## Architecture

![EV Charging Data Engineering Architecture](architecture.png)

## Technology Stack

- Python - Data generation and scripting
- PySpark - Batch processing and transformations
- Apache Kafka - Real-time event ingestion
- Spark Structured Streaming - Real-time data processing
- Azure Data Factory - Data ingestion and pipeline orchestration
- Azure Data Lake Storage Gen2 - Cloud data storage
- Azure Synapse Analytics - SQL analytics and data serving
- Power BI - Data visualization and dashboards
- Git & GitHub - Version control and project management

## Batch Data Pipeline

The batch pipeline processes historical EV charging transaction data using PySpark.

### Pipeline Flow

Raw CSV Data<br>
↓<br>
Bronze Layer<br>
↓<br>
Silver Layer<br>
↓<br>
Gold Layer<br>
↓<br>
Business Analytics

### Bronze Layer

Raw charging transaction data is ingested into the Bronze layer with minimal transformation.

### Silver Layer

The Silver layer performs:

- Data type conversion
- Null-value validation
- Duplicate removal
- Energy validation
- Payment validation
- Timestamp validation
- Status validation

### Gold Layer

The Gold layer creates business-ready analytical datasets:

- Daily charging analytics
- Station analytics
- Customer analytics

### Current Batch Dataset

- Customers: 1,000
- Vehicles: 1,500
- Stations: 50
- Charging Transactions: 100,000

## Real-Time Streaming Pipeline

The real-time pipeline processes EV charging events using Apache Kafka and Spark Structured Streaming.

### Pipeline Flow

EV Charging Events<br>
↓<br>
Kafka<br>
↓<br>
Spark Structured Streaming<br>
↓<br>
Bronze Layer<br>
↓<br>
Silver Layer<br>
↓<br>
Gold Layer<br>
↓<br>
Business Analytics

### Kafka

Apache Kafka is used as the real-time event ingestion layer.

The project uses the Kafka topic:

ev-charging-events

Each event contains:

- Transaction ID
- Customer ID
- Vehicle ID
- Station ID
- Start time
- Energy consumed
- Amount paid
- Charging status

### Streaming Silver Layer

The Silver streaming pipeline validates incoming events by checking:

- Required IDs are not null
- Energy consumption is greater than zero
- Payment amount is non-negative
- Charging status is valid
- Duplicate transaction IDs are checked

### Streaming Gold Layer

The Gold streaming pipeline generates analytics for:

- Charging stations
- Customers
- Vehicles

## Data Quality

The project applies validation rules to maintain reliable analytical data.

### Batch Data Quality

- Required IDs must not be null
- Energy consumption must be greater than zero
- Payment amount must be non-negative
- Timestamps must be valid
- Duplicate transactions are removed
- Charging status must contain valid values

### Streaming Data Quality

- Required IDs are validated
- Invalid energy values are filtered
- Invalid payment values are filtered
- Invalid charging statuses are filtered
- Duplicate transaction IDs are checked

## Medallion Architecture

The project follows the Medallion Architecture.

- Bronze - Raw ingested data
- Silver - Cleaned and validated data
- Gold - Business-ready analytical data

## Project Structure

EV_Vehicles/

data/

- raw/
- processed/
- output/

src/

- data_generation/
- batch/
- streaming/

notebooks/
tests/
config/
architecture.png
README.md
.gitignore

## Current Implementation

The local development pipeline currently includes:

- Synthetic EV charging data generation
- PySpark batch processing
- Bronze, Silver, and Gold layers
- Apache Kafka event streaming
- Spark Structured Streaming
- Streaming Bronze, Silver, and Gold processing
- Station analytics
- Customer analytics
- Vehicle analytics
- Data quality validation
- Git and GitHub version control

## Planned Cloud Deployment

The next stage of the project will extend the local pipeline to Azure.

### Batch Cloud Pipeline

Historical Data<br>
↓<br>
Azure Data Factory<br>
↓<br>
Azure Data Lake Storage Gen2<br>
↓<br>
Bronze → Silver → Gold<br>
↓<br>
Azure Synapse Analytics<br>
↓<br>
Power BI

### Real-Time Pipeline Flow

EV Charging Events<br>
↓<br>
Kafka<br>
↓<br>
Spark Structured Streaming<br>
↓<br>
Bronze Layer<br>
↓<br>
Silver Layer<br>
↓<br>
Gold Layer<br>
↓<br>
Business Analytics

## Key Analytics

The platform is designed to provide insights into:

- Total energy consumption
- Charging revenue
- Charging sessions
- Station utilization
- Customer charging behavior
- Vehicle charging behavior
- Charging demand

## Technologies

Python • PySpark • Apache Kafka • Spark Structured Streaming • Azure Data Factory • Azure Data Lake Storage Gen2 • Azure Synapse Analytics • Power BI • Git • GitHub
