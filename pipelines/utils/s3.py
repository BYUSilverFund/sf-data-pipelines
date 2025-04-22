import boto3
from io import BytesIO
from dotenv import load_dotenv
import polars as pl
import pyarrow.parquet as pq

def upload_df_to_s3(df: pl.DataFrame, bucket_name: str, file_name: str) -> None:
    load_dotenv(override=True)

    # Convert dataframe to parquet buffer
    table = df.to_arrow()
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Instantiate client
    s3_client = boto3.client('s3')

    # Upload
    s3_client.upload_fileobj(buffer, bucket_name, file_name)

    print(f"File uploaded to s3://{bucket_name}/{file_name}")
