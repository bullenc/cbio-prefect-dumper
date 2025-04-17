from rfam_dumper.dumper import test_db_connection, create_dump
from prefect import flow

DB_CONFIG = {
    "host": "mysql-rfam-public.ebi.ac.uk",
    "user": "rfamro",
    "password": "",
    "database": "Rfam",
    "port": 4497,
}

if __name__ == "__main__":
    test_db_connection(**DB_CONFIG)
    create_dump(**DB_CONFIG, dump_family_only=True)


@flow(name="rfam-dump-flow")
def rfam_dump_flow():
    DB_CONFIG = {
        "host": "mysql-rfam-public.ebi.ac.uk",
        "user": "rfamro",
        "password": "",
        "database": "Rfam",
        "port": 4497,
    }

    test_db_connection(**DB_CONFIG)
    create_dump(**DB_CONFIG, dump_family_only=True)
