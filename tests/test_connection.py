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
