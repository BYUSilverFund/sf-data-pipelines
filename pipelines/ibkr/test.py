import pipelines.ibkr.aws as aws
from rich import print

print(
    aws.S3().list_files('ibkr-historical-data')
)