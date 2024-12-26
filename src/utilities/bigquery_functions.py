from google.api_core import retry
from google.cloud.exceptions import NotFound
import pandas as pd
import logging
from typing import Union, List, Dict, Optional
from datetime import datetime
import json
import os
from pathlib import Path
import pyarrow

class BigQueryUploader:
    """A class to handle BigQuery upload operations with proper error handling and logging."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_path: Optional[str] = None,
        location: str = "US"
    ):
        """
        Initialize BigQuery uploader.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            credentials_path: Path to service account credentials JSON file
            location: Dataset location (default: "US")
        """
        # Set up logging
        self.logger = self._setup_logger()

        # Set credentials
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        # Initialize client
        try:
            self.client = bigquery.Client(project=project_id)
            self.logger.info(f"Successfully initialized BigQuery client for project: {project_id}")
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location

    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger("BigQueryUploader")
        logger.setLevel(logging.INFO)

        # Create handlers
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler("bigquery_uploader.log")
        c_handler.setLevel(logging.INFO)
        f_handler.setLevel(logging.INFO)

        # Create formatters and add to handlers
        format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        c_format = logging.Formatter(format_str)
        f_format = logging.Formatter(format_str)
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)

        return logger

    @retry.Retry()
    def create_dataset_if_not_exists(self) -> None:
        """Create dataset if it doesn't exist."""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.info(f"Dataset {dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            self.logger.info(f"Created dataset {dataset_ref}")

    def validate_schema(
        self,
        df: pd.DataFrame,
        schema: List[bigquery.SchemaField]
    ) -> bool:
        """
        Validate DataFrame schema against BigQuery schema.

        Args:
            df: Pandas DataFrame to validate
            schema: List of BigQuery SchemaField objects

        Returns:
            bool: True if valid, raises ValueError if invalid
        """
        try:
            for field in schema:
                if field.name not in df.columns:
                    raise ValueError(f"Missing column {field.name} in DataFrame")

                # Basic type checking
                bq_type = field.field_type.lower()
                df_type = df[field.name].dtype

                # Add type validation rules as needed
                if bq_type == "string" and not pd.api.types.is_string_dtype(df_type):
                    raise ValueError(f"Column {field.name} should be string type")
                elif bq_type == "integer" and not pd.api.types.is_integer_dtype(df_type):
                    raise ValueError(f"Column {field.name} should be integer type")
                # Add more type validations as needed

            return True
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            raise

    @retry.Retry()
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        schema: Optional[List[bigquery.SchemaField]] = None,
        write_disposition: str = "WRITE_APPEND",
        partition_field: Optional[str] = None
    ) -> None:
        """
        Upload DataFrame to BigQuery table.

        Args:
            df: Pandas DataFrame to upload
            table_id: Target table ID
            schema: Optional BigQuery table schema
            write_disposition: Write disposition (default: WRITE_APPEND)
            partition_field: Optional field for table partitioning
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        try:
            # Ensure dataset exists
            self.create_dataset_if_not_exists()

            # Validate schema if provided
            if schema:
                self.validate_schema(df, schema)

            # Configure job
            job_config = bigquery.LoadJobConfig()
            if schema:
                job_config.schema = schema
            job_config.write_disposition = write_disposition

            # Set up partitioning if specified
            if partition_field:
                job_config.time_partitioning = bigquery.TimePartitioning(
                    field=partition_field
                )

            # Upload data
            start_time = datetime.now()
            job = self.client.load_table_from_dataframe(
                df,
                table_ref,
                job_config=job_config
            )
            job.result()  # Wait for job to complete

            # Log success
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"Successfully uploaded {len(df)} rows to {table_ref} "
                f"in {duration:.2f} seconds"
            )

        except Exception as e:
            self.logger.error(f"Failed to upload data to {table_ref}: {str(e)}")
            raise

    def upload_file(
        self,
        file_path: Union[str, Path],
        table_id: str,
        schema: Optional[List[bigquery.SchemaField]] = None,
        write_disposition: str = "WRITE_APPEND",
        partition_field: Optional[str] = None,
        file_type: str = "csv"
    ) -> None:
        """
        Upload file to BigQuery table.

        Args:
            file_path: Path to input file
            table_id: Target table ID
            schema: Optional BigQuery table schema
            write_disposition: Write disposition (default: WRITE_APPEND)
            partition_field: Optional field for table partitioning
            file_type: File type (default: "csv")
        """
        try:
            # Read file into DataFrame based on type
            if file_type.lower() == "csv":
                df = pd.read_csv(file_path)
            elif file_type.lower() == "parquet":
                df = pd.read_parquet(file_path)
            elif file_type.lower() == "json":
                df = pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            # Upload DataFrame
            self.upload_dataframe(
                df,
                table_id,
                schema,
                write_disposition,
                partition_field
            )

        except Exception as e:
            self.logger.error(f"Failed to upload file {file_path}: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    # Example schema
    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    # Initialize uploader
    uploader = BigQueryUploader(
        project_id="your-project-id",
        dataset_id="your_dataset",
        credentials_path="path/to/credentials.json"
    )

    # Example DataFrame upload
    df = pd.DataFrame({
        "name": ["John", "Jane"],
        "age": [30, 25],
        "timestamp": [datetime.now(), datetime.now()]
    })

    # Upload with partitioning
    uploader.upload_dataframe(
        df=df,
        table_id="your_table",
        schema=schema,
        write_disposition="WRITE_APPEND",
        partition_field="timestamp"
    )

    # Example file upload
    uploader.upload_file(
        file_path="data.csv",
        table_id="your_table",
        schema=schema,
        partition_field="timestamp"
    )