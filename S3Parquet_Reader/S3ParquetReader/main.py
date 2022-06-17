import boto3
import pandas as pd
import io

def parquetReader(bucket_name):
    # set this in order to view entire dataframe
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    # Connect to your S3 bucket
    S3_client = boto3.resource("s3")
    my_bucket = S3_client.Bucket(bucket_name)

    # get the file which are the parquet files
    parquet_files = [file.key for file in my_bucket.objects.all() if file.key.endswith(".parquet")]

    # Start to read parquet data and then load into dataframe
    students_df = []
    for file in parquet_files:
        obj = S3_client.Object(bucket_name, file)
        data = obj.get()['Body'].read()
        students_df.append(pd.read_parquet(io.BytesIO(data)))

    print(pd.concat(students_df, ignore_index=True))


if __name__ == '__main__':
    bucket_name = "zavierstorage"
    parquetReader(bucket_name)

