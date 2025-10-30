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

    def initialize_postgres_db(self, data_access, working_folder):
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
        sql_path = os.path.join(file_path, "..", "queries", "00_create_base.sql")

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
            inside_function = False
            for line in sql_content.splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("--"):
                    continue
                if "$$" in stripped:
                    inside_function = not inside_function
                    current_stmt.append(line)
                    continue
                if not inside_function and stripped.endswith(";"):
                    current_stmt.append(line)
                    statements.append("\n".join(current_stmt).strip())
                    current_stmt = []
                else:
                    current_stmt.append(line)
            if current_stmt:
                statements.append("\n".join(current_stmt).strip())

            print(f"🛠️ Executing {len(statements)} statements from 00_create_base.sql ...")

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
                INSERT INTO banorte_load.cutoff_years (year_value)
                VALUES ({self.today.year})
                ON CONFLICT DO NOTHING;
            """
            cur.execute(insert_year)
            print(f"{Fore.GREEN}✅ Database initialized and cutoff_year {self.today.year} inserted (if not already present).{Style.RESET_ALL}")

            # Listar tablas creadas
            print(f"{Fore.CYAN}📋 Current tables in 'banorte_load':{Style.RESET_ALL}")
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'banorte_load';")
            for row in cur.fetchall():
                print(f"   - {row[0]}")

        except Exception as e:
            print(f"{Fore.RED}❌ Error executing script: {e}{Style.RESET_ALL}")
            raw_conn.close()
            return False

        raw_conn.close()
        print(f"{Fore.GREEN}🎯 Initialization complete and connection closed.{Style.RESET_ALL}")

        streamlit_path = os.path.join(file_path, "concept_filing.py")
        try:
            subprocess.run([sys.executable, "-m", "streamlit", "run", streamlit_path], check=True)
        except Exception as e:
            print(f"❌ Error al ejecutar Streamlit: {e}")

        return True
        
    def initialize(self, root_folder):
        repo_name = os.path.basename(root_folder)
        print(f"Repository Name: {repo_name}")
        folder_name = "MAIN_PATH"
        env_path = os.path.join(root_folder, ".env")
        # --- CASE 1: .env exists ---
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path)
            working_folder = os.getenv(folder_name)

            if working_folder and os.path.isdir(working_folder):
                print(f"✅ Loaded working folder from .env: {working_folder}")
                return working_folder
            else:
                print("⚠️ MAIN_PATH not found or invalid in .env.")
        else:
            print("⚠️ No .env file found.")

        # --- CASE 2: .env missing or invalid path ---
        while True:
            working_folder = input("Enter the path for repository files (leave empty to use current directory): ").strip()

            if not working_folder:
                use_current = input("Use current directory as working folder? (y/n): ").strip().lower()
                if use_current == "y":
                    working_folder = os.path.join(root_folder, repo_name)
                    break
            elif os.path.isdir(working_folder):
                working_folder = os.path.join(working_folder, repo_name)
                break
            else:
                print("❌ Invalid path. Please try again.")

        # --- Save to .env ---
        with open(env_path, "w") as f:
            f.write(f'{folder_name}="{working_folder}"\n')
        print(f"💾 Recorded {folder_name} in .env → {working_folder}")

        return working_folder