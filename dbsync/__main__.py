import os
from datetime import datetime

from dotenv import load_dotenv

from dbsync import backup, seed, restore, insert_csv, export_csv, storage_cleanup


def main():
    os.makedirs("storage", exist_ok=True)
    csv_file = "./storage/jachi_bills_today.csv"

    print("Seed Start")
    seed(os.getenv("AWS_DATABASE_URL"))
    print("Seed Completed")

    print("Export Start")
    export_csv(os.getenv("MAIN_DATABASE_URL"), csv_file, "jachi_bills")
    print("Export Completed")

    print("Import Start")
    insert_csv(os.getenv("AWS_DATABASE_URL"), csv_file, "jachi_bills")
    print("Import Completed")

    print("Cleanup Start")
    storage_cleanup()
    print("Cleanup Completed")


if __name__ == "__main__":
    start_time = datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] Starting async main function")
    load_dotenv()
    main()
    end_time = datetime.now()
    print(f"[{end_time.strftime('%Y-%m-%d %H:%M:%S')}] Starting async main function")
    duration = end_time - start_time
    print(f"Took {duration.seconds} seconds")
