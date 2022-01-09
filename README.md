# Example Data Processing
This is my example to process data from application into data warehouse.
I Have use Pyspark and Beam for data processing and snipped some code into this repository.

# Data Process Workflow
1. Read Data from Application Database (eg. MySQL)
2. Save Original data into file (eg. Parquet)
3. I need to get a column that contain JSON string to transform into valuable record.
4. Read JSON string into dict. then process to create new record.
5. Save into Data Warehouse.

On Beam pipeline, I integrated with GCP stack with Dataflow, GCS and Bigquery.
