import os
from prefect_shell import ShellOperation
from datetime import datetime
from pytz import timezone
from prefect import flow, task
import boto3, json
from botocore.exceptions import ClientError


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string

@task(name="get_secret")
def get_secret(env_name: str):
    """Get the secret from AWS Secrets Manager.

    Args:
        env_name (str): Environment name (e.g., dev, prod)

    Raises:
        e: ClientError

    Returns:
        dict: JSON object with credentials
    """

    region_name = "us-east-1"

    if env_name == "dev":
        secret_name = "ccdicbio-dev-rds"
        
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # retreive the secret
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return secret

@task(name="create_dump")
def create_dump(
    host: str,
    username: str,
    password: str,
    engine: str = "mysql",
    dbClusterIdentifier: str = "",
    port=3306,
    output_dir="/usr/local/data/dumps",
):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = get_time()
    dump_file = os.path.join(output_dir, f"{dbClusterIdentifier}_dump_{timestamp}.sql")

    command = [
        "mysqldump",
        f"--host={host}",
        f"--port={port}",
        f"--user={username}",
        f"--password={password}",
        "--single-transaction",
        "--skip-lock-tables",
        "--databases",
        dbClusterIdentifier,
    ]

    try:
        with open(dump_file, "w") as f:
            """process = subprocess.run(
                command, stdout=f, stderr=subprocess.PIPE, check=False, shell=False
            )
            if process.returncode != 0:
                print(f"Error code: {process.returncode}")
                print(f"Error message: {process.stderr.decode()}")
                raise subprocess.CalledProcessError(
                    process.returncode, command, process.stderr
                )"""
            result = ShellOperation(
                commands=[
                    " ".join(command) + f" > {dump_file}"
                ],
                stream_output=True,
                log_output=False,
            ).run()
            
            print(result)
            if result.get("exit_code", 0) != 0:
                raise Exception(f"ShellOperation failed with exit code {result.get('exit_code')}: {result.get('stderr', 'No error message')}")
        print(f"✅ Dump successful: {dump_file}")
        return dump_file
    except Exception as err:
        print(f"❌ mysqldump failed: {err}")
        raise

@task(name="upload_to_s3")
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

@flow(name="cbio-dump-flow-test", log_prints=True)
def export_db(
    env_tier: str, bucket_name: str
):
    """Execute database export and upload to S3.

    Args:
        env_tier (str): Tier to perform export on (e.g., dev, prod)
        bucket_name (str, optional): Bucket name to upload to. Defaults to "dev".
    """
    
    # retrieve and load creds
    creds_string = get_secret(env_tier)
    creds = json.loads(creds_string)

    dump_file_path = create_dump(**creds)
    upload_to_s3(dump_file_path, bucket_name)

    #remove the dump file
    os.remove(dump_file_path)
    print(f"✅ Removed dump file: {dump_file_path}")


if __name__ == "__main__":
    export_db()
