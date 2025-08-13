import boto3
import polars as pl
from io import StringIO, BytesIO
from sqlalchemy import create_engine
import psycopg2
from rich import print
import jinja2

class S3:

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> None:
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def get_file(self, bucket_name: str, file_key: str) -> pl.DataFrame:
        s3_object = self.client.get_object(Bucket=bucket_name, Key=file_key)

        file_content = s3_object['Body'].read().decode('utf-8')

        return pl.read_csv(StringIO(file_content))
    
    def drop_file(self, file_name: str, bucket_name: str, file_data: pl.DataFrame) -> None:
        csv_buffer = StringIO()

        file_data.write_csv(csv_buffer)

        csv_bytes = BytesIO(csv_buffer.getvalue().encode())

        self.client.upload_fileobj(csv_bytes, bucket_name, file_name)

    def list_files(self, bucket_name: str):
        file_paths = []

        response = self.client.list_objects_v2(Bucket=bucket_name)

        for object in response['Contents']:
            file_path = bucket_name + "/" + object['Key']

            # Only append .csv files
            if file_path[-4:] == '.csv':
                file_paths.append(file_path)

        return file_paths
    
class SecretsManager:

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> None:
        self.client = boto3.client(
            'secretsmanager',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def get_secret(self, secret_id: str):
        return self.client.get_secret_value(SecretId=secret_id)

class RDS:

    def __init__(self, db_endpoint: str, db_name: str, db_user: str, db_password: str, db_port: str):
        self.db_endpoint = db_endpoint
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port

        try:
            # Establish the connection
            self.connection = psycopg2.connect(
                host=self.db_endpoint,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port
            )

            # Create a cursor object
            self.cursor = self.connection.cursor()

            # Create the SQLAlchemy engine
            db_url = f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_endpoint}:{self.db_port}/{self.db_name}'
            self.engine = create_engine(db_url)

        except Exception as e:
            print(f'Error: {e}')
    
    def execute(self, query_string: str) -> list[tuple[any]]:
        
        self.cursor.execute(query_string)
        
        if self.cursor.description:  # Means it's a SELECT or returning rows
            rows = self.cursor.fetchall()
            return rows
        
        else:
            self.connection.commit()
            return None
        
    def execute_sql_file(self, file_name: str) -> list[tuple[any]]:

        with open(file_name, 'r') as file:
            self.cursor.execute(file.read())
            
            if self.cursor.description:  # Means it's a SELECT or returning rows
                rows = self.cursor.fetchall()
                return rows
            
            else:
                self.connection.commit()
                return None
            
    def execute_sql_template_file(self, file_name: str, params: dict) -> list[tuple[any]]:
        with open(file_name, 'r') as file:
            template = jinja2.Template(source=file.read())

            self.cursor.execute(template.render(params))
            
            if self.cursor.description:  # Means it's a SELECT or returning rows
                rows = self.cursor.fetchall()
                return rows
            
            else:
                self.connection.commit()
                return None
        
    def stage_dataframe(self, df: pl.DataFrame, table_name: str):
        df.write_database(
            table_name=table_name, 
            connection=self.engine, 
            if_table_exists='replace', 
        )

if __name__ == '__main__':
    import dotenv
    import os

    dotenv.load_dotenv(override=True)

    # secrets_mangager = SecretsManager(
    #     aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
    #     aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    #     region_name=os.getenv('COGNITO_REGION'),
    # )

    # print(
    #     secrets_mangager.get_secret('ibkr-secrets')
    # )
