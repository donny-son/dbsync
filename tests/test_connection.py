import os
import gzip

from dotenv import load_dotenv
from sh import pg_dump, psql, pg_restore

load_dotenv()


def test_commands_exists():
    psql_process = psql("-V")
    pg_dump_process = pg_dump("-V")
    pg_restore_process = pg_restore("-V")
    assert psql_process.exit_code == 0
    assert pg_dump_process.exit_code == 0
    assert pg_restore_process.exit_code == 0


def test_psql_connection():
    main_db_process = psql(os.getenv("MAIN_DATABASE_URL"), "SELECT 1")
    aws_db_process = psql(os.getenv("AWS_DATABASE_URL"), "SELECT 1")
    assert main_db_process.exit_code == 0
    assert aws_db_process.exit_code == 0


def test_get_csv():
    with open(f"./storage/test_jachi_bills.csv", "w") as f:
        proc = psql(
            os.getenv("MAIN_DATABASE_URL"),
            "-c",
            f"\\COPY (SELECT * FROM jachi_bills where crawl_dt = (SELECT CURRENT_DATE) LIMIT 5) TO STDOUT WITH CSV HEADER",
            _out=f,
        )
        assert proc.exit_code == 0, f"copy csv failed"


def test_insert_from_csv():
    with open(f"./storage/test_jachi_bills.csv", "r") as f:
        proc = psql(
            os.getenv("AWS_DATABASE_URL"),
            "-c",
            f"\\COPY jachi_bills FROM STDIN WITH CSV HEADER",
            _in=f,
        )
        assert proc.exit_code == 0, f"copy csv failed"
        read = psql(
            os.getenv("AWS_DATABASE_URL"), "-c", f"SELECT * FROM jachi_bills LIMIT 5;"
        )
        assert read.exit_code == 0, f"read jachi_bills table failed"
