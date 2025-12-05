import os
from dotenv import load_dotenv
from colorama import Fore, Style, init
from sqlalchemy import create_engine, text
from datetime import date
import pandas as pd  
from urllib.parse import urlparse
import psycopg2
import sys
import subprocess

class INITIALIZE:
    def __init__(self):
        print(f"{Fore.BLUE}CLASS INITIALIZE{Style.RESET_ALL}")

    def initialize_postgres_db(self, data_access, query_file, schema):
        print(f"{Fore.BLUE}Initializing PostgreSQL database{Style.RESET_ALL}")
        self.today = date.today()

        # Extraer datos de la URL
        db_url = data_access["sql_workflow"]
        parsed = urlparse(db_url)
        conn_params = {
            "dbname": parsed.path.lstrip("/"),
            "user": parsed.username,
            "password": parsed.password,
            "host": parsed.hostname,
            "port": parsed.port or 5432,
        }

        # Conexión directa psycopg2
        try:
            raw_conn = psycopg2.connect(**conn_params)
            raw_conn.autocommit = True
            cur = raw_conn.cursor()
            print(f"{Fore.GREEN}✅ Direct PostgreSQL connection established.{Style.RESET_ALL}")
        except Exception as e:
            print(f"❌ Error creating raw PostgreSQL connection: {e}")
            return False

        # Leer el resto del script
        file_path = os.path.dirname(__file__)
        sql_path = os.path.join(file_path, "..", "queries", f"{query_file}")

        if not os.path.exists(sql_path):
            print(f"❌ SQL file not found: {sql_path}")
            raw_conn.close()
            return False

        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Dividir respetando $$, ignorar comentarios
            statements = []
            current_stmt = []
            inside_block = False  # funciones y DO $$ ... $$

            for line in sql_content.splitlines():
                stripped = line.strip()
                # Saltar líneas vacías o comentarios
                if not stripped or stripped.startswith("--"):
                    continue

                # Manejo de bloques $$ ... $$
                if "$$" in stripped:
                    if inside_block:
                        # Cerrando bloque: agregamos línea y cerramos el statement
                        current_stmt.append(line)
                        statements.append("\n".join(current_stmt).strip())
                        current_stmt = []
                        inside_block = False
                    else:
                        # Abriendo bloque
                        inside_block = True
                        current_stmt.append(line)
                    continue

                # Statements normales terminados en ;
                if not inside_block and stripped.endswith(";"):
                    current_stmt.append(line)
                    statements.append("\n".join(current_stmt).strip())
                    current_stmt = []
                else:
                    current_stmt.append(line)

            if current_stmt:
                statements.append("\n".join(current_stmt).strip())
            # Reemplazamos el esquema     
            statements = [
                stmt.replace("{schema}", schema)
                for stmt in statements
            ]
            print(f"🛠️ Executing {len(statements)} statements from {query_file} ...")

            for i, stmt in enumerate(statements, 1):
                try:
                    print(f"{Fore.YELLOW}▶️  Executing statement {i}/{len(statements)}...{Style.RESET_ALL}")
                    print(f"🧾  {stmt[:60].replace(chr(10),' ')}...")  # muestra inicio del query
                    cur.execute(stmt)
                except Exception as e:
                    print(f"{Fore.RED}❌ Error in statement {i}: {e}{Style.RESET_ALL}")
                    print(f"--- SQL ---\n{stmt[:400]}...\n")
                    continue

            # Insertar año actual
            insert_year = f"""
                INSERT INTO {schema}.cutoff_years (year_value)
                VALUES ({self.today.year})
                ON CONFLICT DO NOTHING;
            """
            cur.execute(insert_year)
            print(f"{Fore.GREEN}✅ Database initialized and cutoff_year {self.today.year} inserted (if not already present).{Style.RESET_ALL}")

            # Listar tablas creadas
            print(f"{Fore.CYAN}📋 Current tables in '{schema}':{Style.RESET_ALL}")
            cur.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}';")
            for row in cur.fetchall():
                print(f"   - {row[0]}")

        except Exception as e:
            print(f"{Fore.RED}❌ Error executing script: {e}{Style.RESET_ALL}")
            raw_conn.close()
            return False

        raw_conn.close()
        print(f"{Fore.GREEN}🎯 Initialization complete and connection closed.{Style.RESET_ALL}")