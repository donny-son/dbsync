import os
import glob

from sh import pg_dump, psql, pg_restore


def seed(dburl: str | None):
    if not dburl:
        raise ValueError("`dburl` is missing. Please check your .env file")
    proc = psql(dburl, "-f", "./sql/seed.sql")
    if proc.exit_code == 0:
        return True
    else:
        raise RuntimeError("seeding failed")


def backup(dburl: str):
    with open("backup.dump", "w") as f:
        proc = pg_dump(dburl, "--create", "-Fc", _out=f)
        assert proc.exit_code == 0, "backup failed"


def restore(host: str, user: str, dbname: str):
    try:
        drop_database = psql(
            "-h", host, "-U", user, "-c", f"DROP DATABASE  IF EXISTS {dbname};"
        )
        create_database = psql(
            "-h", host, "-U", user, "-c", f"CREATE DATABASE {dbname};"
        )
        assert drop_database.exit_code == 0, "drop database failed"
        assert create_database.exit_code == 0, "create database failed"
    except Exception as e:
        raise e

    restore_database = pg_restore("-h", host, "-U", user, "-d", dbname, "backup.dump")
    assert restore_database.exit_code == 0, "restore database failed"


def insert_csv(dburl: str | None, file: str, table_name: str):
    if not dburl:
        raise ValueError("`dburl` is missing. Please check your .env file")
    with open(file, "r") as f:
        proc = psql(
            dburl, "-c", f"\\COPY {table_name} FROM STDIN WITH CSV HEADER", _in=f
        )
        assert proc.exit_code == 0, f"copy csv to table failed"


def export_csv(dburl: str | None, file: str, table_name: str):
    with open(file, "w") as f:
        proc = psql(
            dburl,
            "-c",
            f"\\COPY (SELECT * FROM {table_name} where crawl_dt = (SELECT CURRENT_DATE)) TO STDOUT WITH CSV HEADER",
            _out=f,
        )
        assert proc.exit_code == 0, f"copy csv from table failed"


def storage_cleanup():
    files = glob.glob("./storage/*")
    for f in files:
        os.remove(f)
