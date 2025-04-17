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
        "--single-transaction",  # ✅ add this
        "--skip-lock-tables",  # ✅ optional, reinforces the same thing
        "--databases",
        database,
    ]

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
        output = subprocess.check_output(["mysqldump", "--version"]).decode().strip()
        print(f"mysqldump version: {output}")
    except Exception as err:
        print(f"❌ mysqldump failed: {err}")
        raise


@flow(name="rfam-dump-flow-test", log_prints=True)
def test_dump_flow():
    DB_CONFIG = {
        "host": "relational.fel.cvut.cz",
        "user": "guest",
        "password": "ctu-relational",
        "database": "Financial_std",
        "port": 3306,
    }

    create_dump(**DB_CONFIG, dump_family_only=True)


if __name__ == "__main__":
    DB_CONFIG = {
        "host": "relational.fel.cvut.cz",
        "user": "guest",
        "password": "ctu-relational",
        "database": "Financial_std",
        "port": 3306,
    }

    create_dump(**DB_CONFIG, dump_family_only=True)
