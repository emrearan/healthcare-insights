import os
import sys
import psycopg2

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PW = os.getenv("POSTGRES_PW")


def run_sql_file(filename):
    with open(f"sql/{filename}", "r") as f:
        sql = f.read()

    conn = psycopg2.connect(
        dbname="demo",
        user=POSTGRES_USER,
        password=POSTGRES_PW,
        host="postgres",
        port=5432,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()
    print(f"{filename} executed successfully.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_script.py <sql_file>")
        sys.exit(1)
    run_sql_file(sys.argv[1])
