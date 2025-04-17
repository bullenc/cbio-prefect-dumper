import os
import subprocess
import mysql.connector
from datetime import datetime
from prefect import flow, task


@task
def test_db_connection(host, user, password, database, port=3306):
    try:
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )
        print("✅ Connected to the database.")
        conn.close()
    except mysql.connector.Error as err:
        print(f"❌ Connection failed: {err}")
        raise


@task
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

    command = [
        "mysqldump",
        f"--host={host}",
        f"--port={port}",
        f"--user={user}",
        "--skip-lock-tables",
        database,
    ]

    if tables:
        command.append(tables)

    try:
        with open(dump_file, "w") as f:
            subprocess.run(command, stdout=f, check=True)
        print(f"✅ Dump successful: {dump_file}")
    except subprocess.CalledProcessError as err:
        print(f"❌ mysqldump failed: {err}")
        raise
