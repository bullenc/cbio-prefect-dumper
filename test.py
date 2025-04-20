import os
import subprocess
from datetime import datetime
from prefect import flow
import boto3, json
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "test/db/creds"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return secret


def create_dump(
    host,
    user,
    password,
    database,
    port=3306,
    output_dir="./dumps",
):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_file = os.path.join(output_dir, f"{database}_dump_{timestamp}.sql")

    command = [
        "mysqldump",
        f"--host={host}",
        f"--port={port}",
        f"--user={user}",
        f"--password={password}",
        "--single-transaction",
        "--skip-lock-tables",
        "--databases",
        database,
    ]

    try:
        with open(dump_file, "w") as f:
            process = subprocess.run(
                command, stdout=f, stderr=subprocess.PIPE, check=False
            )
            if process.returncode != 0:
                print(f"Error code: {process.returncode}")
                print(f"Error message: {process.stderr.decode()}")
                raise subprocess.CalledProcessError(
                    process.returncode, command, process.stderr
                )
        print(f"✅ Dump successful: {dump_file}")
        return dump_file  # ← Return the dump file path
    except Exception as err:
        print(f"❌ mysqldump failed: {err}")
        raise


@flow(name="rfam-dump-flow-test", log_prints=True)
def test_dump_flow(
    secret_name: str = "test/db/creds", bucket_name: str = "fake_bucket"
):
    creds_string = get_secret()
    # print(f"creds: {creds}")
    DB_CONFIG = {
        "host": "relational.fel.cvut.cz",
        "user": "guest",
        "password": "ctu-relational",
        "database": "Financial_std",
        "port": 3306,
    }
    # print(f"DBCONIG: {DB_CONFIG}")
    creds = json.loads(creds_string)
    dump_file_path = create_dump(**creds)
    upload_to_s3(dump_file_path, bucket_name)


def upload_to_s3(file_path, bucket_name, region_name="us-east-1"):
    s3 = boto3.client("s3", region_name=region_name)
    file_name = file_path.split("/")[-1]
    s3_key = f"dump_folder/{file_name}"

    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
        raise


if __name__ == "__main__":
    test_dump_flow()
