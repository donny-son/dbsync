import os
import gzip

from dotenv import load_dotenv
from sh import pg_dump


def backup(dburl: str):
    with gzip.open("backup.gz", "wb") as f:
        pg_dump(dburl, _out=f)


def main():
    print("Starting backup")
    load_dotenv()
    backup(os.getenv("MAIN_DATABASE_URL"))
    print("Backup complete")
