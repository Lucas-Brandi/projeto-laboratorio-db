"""
Roda o(s) script(s) SQL em sql/ contra o banco configurado no .env.
Uso:
    python src/init_db.py                          # roda todos os .sql de sql/
    python src/init_db.py sql/003_create_dw.sql    # roda só o(s) arquivo(s) indicado(s)
"""

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode=os.environ.get("DB_SSLMODE", "require"),
    )


def main():
    if len(sys.argv) > 1:
        sql_files = [Path(arg) for arg in sys.argv[1:]]
    else:
        sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        print("Nenhum arquivo .sql encontrado em sql/.")
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            for sql_file in sql_files:
                print(f"Executando {sql_file.name}...")
                cur.execute(sql_file.read_text(encoding="utf-8"))
        conn.commit()

    print("Schema criado/atualizado com sucesso.")


if __name__ == "__main__":
    main()
