import os
import subprocess
import mysql.connector
from datetime import datetime
from prefect import flow


def create_dump(
    host,
    user,
    password,
    database,
    port=3306,
    dump_family_only=True,
    output_dir="./dumps",
):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if dump_family_only:
        dump_file = os.path.join(output_dir, f"{database}_family_{timestamp}.sql")
        tables = "family"
    else:
        dump_file = os.path.join(output_dir, f"{database}_full_{timestamp}.sql")
        tables = ""

    # command = [
    #     "mysqldump",
    #     f"--host={host}",
    #     f"--port={port}",
    #     f"--user={user}",
    #     "--skip-lock-tables",
    #     "--column-statistics=0",
    #     database,
    # ]
    command = ["mysqldump", "--version"]

    # if tables:
    #     command.append(tables)

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
    except Exception as err:
        print(f"❌ mysqldump failed: {err}")
        raise


@flow(name="rfam-dump-flow-test", log_prints=True)
def rfam_dump_flow():
    DB_CONFIG = {
        "host": "mysql-rfam-public.ebi.ac.uk",
        "user": "rfamro",
        "password": "",
        "database": "Rfam",
        "port": 4497,
    }

    create_dump(**DB_CONFIG, dump_family_only=True)


if __name__ == "__main__":
    DB_CONFIG = {
        "host": "mysql-rfam-public.ebi.ac.uk",
        "user": "rfamro",
        "password": "",
        "database": "Rfam",
        "port": 4497,
    }

    create_dump(**DB_CONFIG, dump_family_only=True)
