import os  # Import the os module
#from utils.helpers import Helper  # Import the Helper class
import yaml
from sqlalchemy import create_engine, text
import pandas as pd
try:
    from .generador_reportes import GeneradorReportes
except ImportError:
    from generador_reportes import GeneradorReportes
try:
    from .conceptos import Conceptos
except ImportError:
    from conceptos import Conceptos

class DataWarehouse:
    def __init__(self, strategy_folder, data_access):
        self.strategy_folder = strategy_folder
        self.mirror_credito_path = os.path.join(self.strategy_folder, 'mirror_credito.pkl')
        self.mirror_debito_path = os.path.join(self.strategy_folder, 'mirror_debito.pkl')
        self.data_access = data_access
        #self.helper = Helper()
        self.generador_reportes = GeneradorReportes(self.data_access, self.strategy_folder)
        self.conceptos = Conceptos(self.strategy_folder, self.data_access)
        folder_root = os.getcwd()
        self.queries_folder = os.path.join(folder_root, 'queries')

    def _get_table_columns(self, engine, schema: str, table: str):
        """Return ordered list of column names for a given schema.table from information_schema."""
        query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [r[0] for r in rows]

    def _compare_table_groups(self, engine, schema: str, tables: list, group_name: str):
        """Compare column sets across a list of tables and print a human-friendly diff."""
        print(f"\n🔎 Verificando columnas en grupo {group_name} ({schema})…")
        cols_by_table = {}
        for t in tables:
            cols = self._get_table_columns(engine, schema, t)
            cols_by_table[t] = cols
            print(f"  • {t}: {len(cols)} columnas")
        # Build union and intersection
        sets = {t: set(cols) for t, cols in cols_by_table.items()}
        all_cols = set().union(*sets.values()) if sets else set()
        common_cols = set.intersection(*sets.values()) if sets else set()
        # Print per-table differences
        for t, s in sets.items():
            missing = common_cols - s
            extras = s - common_cols
            if missing or extras:
                if missing:
                    print(f"    - {t} le faltan: {sorted(missing)}")
                if extras:
                    print(f"    - {t} columnas extra: {sorted(extras)}")
            else:
                print(f"    - {t} ✅ coincide con el conjunto común de columnas")
        # Summary
        if len(common_cols) == len(all_cols) and all(len(s) == len(common_cols) for s in sets.values()):
            print(f"✅ Todas las tablas del grupo {group_name} comparten exactamente las mismas columnas ({len(common_cols)}).")
        else:
            print(f"⚠️ Diferencias detectadas en el grupo {group_name}. Columnas comunes: {len(common_cols)} de {len(all_cols)} totales.")

    def _ensure_schema(self, engine, schema: str):
        # Ensure schema exists using AUTOCOMMIT so it is visible to subsequent pooled connections
        create_stmt = text(f'CREATE SCHEMA IF NOT EXISTS "{schema}" AUTHORIZATION CURRENT_USER')
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(create_stmt)

    def _get_rel_columns(self, engine, schema: str, rel: str):
        """Get ordered columns for a table or view in schema."""
        query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :rel
            ORDER BY ordinal_position
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(query, {"schema": schema, "rel": rel}).fetchall()
        return [r[0] for r in rows]

    def print_columns_summary(self):
        """Print columns for raw and stage relations to confirm names."""
        target_url = self.data_access['local_sql_url']
        engine = create_engine(target_url, pool_pre_ping=True)
        print("\n📋 Columnas en destino (local):")
        raw = ['credito_cerrado', 'credito_corriente', 'debito_cerrado', 'debito_corriente']
        for t in raw:
            cols = self._get_rel_columns(engine, 'raw_banorte', t)
            print(f"  - raw_banorte.{t}: {cols}")
        for v in ['credito_all', 'debito_all']:
            cols = self._get_rel_columns(engine, 'stage_banorte', v)
            print(f"  - stage_banorte.{v}: {cols}")

    def _table_exists(self, engine, schema: str, table: str) -> bool:
        """Check if a table exists in a given schema."""
        query = text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            res = conn.execute(query, {"schema": schema, "table": table}).fetchone()
        return res is not None

    def _replace_without_drop(self, tgt_engine, tgt_schema: str, table: str, df: pd.DataFrame):
        """Replace table contents without DROP, preserving dependent views (TRUNCATE + INSERT)."""
        self._ensure_schema(tgt_engine, tgt_schema)
        # If table exists, TRUNCATE; if not, pandas will create it on first append
        if self._table_exists(tgt_engine, tgt_schema, table):
            with tgt_engine.connect() as conn:
                conn.execute(text(f'TRUNCATE TABLE "{tgt_schema}"."{table}"'))
        if len(df) > 0:
            df.to_sql(table, tgt_engine, schema=tgt_schema, if_exists='append', index=False, method='multi')

    def _append_only_new_rows(self, tgt_engine, tgt_schema: str, table: str, df_src: pd.DataFrame):
        """Append only rows not already present in target, based on row-wise hash of all columns."""
        from pandas.util import hash_pandas_object
        self._ensure_schema(tgt_engine, tgt_schema)
        # If table doesn't exist, create and load everything
        if not self._table_exists(tgt_engine, tgt_schema, table):
            if len(df_src) > 0:
                df_src.to_sql(table, tgt_engine, schema=tgt_schema, if_exists='append', index=False, method='multi')
            return len(df_src)

        # Select target rows with columns in same order as source
        cols = list(df_src.columns)
        cols_quoted = ", ".join([f'"{c}"' for c in cols])
        df_tgt = pd.read_sql(text(f'SELECT {cols_quoted} FROM "{tgt_schema}"."{table}"'), tgt_engine)

        # Compute hashes to detect new rows
        src_hash = hash_pandas_object(df_src[cols], index=False)
        tgt_hash = hash_pandas_object(df_tgt[cols], index=False) if len(df_tgt) > 0 else pd.Series([], dtype=src_hash.dtype)
        tgt_hash_set = set(tgt_hash.tolist())
        mask_new = ~src_hash.isin(tgt_hash_set)
        df_new = df_src.loc[mask_new]

        if len(df_new) > 0:
            # Drop duplicates within the new batch to avoid duplicate inserts if repeated in source
            df_new = df_new.drop_duplicates(subset=cols)
            df_new.to_sql(table, tgt_engine, schema=tgt_schema, if_exists='append', index=False, method='multi')
        return len(df_new)

    def _sync_table(self, src_engine, tgt_engine, src_schema: str, table: str, tgt_schema: str):
        """Synchronize a table applying rules:
        - *_corriente: full replace (TRUNCATE + INSERT) daily
        - *_cerrado: append-only; insert only new rows
        Avoids DROP to keep dependent views valid.
        """
        print(f"⬇️  Extrayendo {src_schema}.{table} …")
        df = pd.read_sql(text(f'SELECT * FROM "{src_schema}"."{table}"'), src_engine)
        # Ensure target schema exists (for new pooled connections)
        self._ensure_schema(tgt_engine, tgt_schema)

        if table.endswith('_corriente'):
            print(f"🔁 Reemplazando contenido de {tgt_schema}.{table} (sin DROP)…")
            self._replace_without_drop(tgt_engine, tgt_schema, table, df)
            print(f"✅ Reemplazado {tgt_schema}.{table} ({len(df)} filas)")
        elif table.endswith('_cerrado'):
            print(f"➕ Insertando nuevas filas en {tgt_schema}.{table}…")
            inserted = self._append_only_new_rows(tgt_engine, tgt_schema, table, df)
            print(f"✅ {tgt_schema}.{table}: {inserted} filas nuevas insertadas; total fuente {len(df)}")
        else:
            # Fallback: conservative replace without drop
            print(f"ℹ️  Tabla {table} sin sufijo esperado; aplicando reemplazo conservador (TRUNCATE + INSERT).")
            self._replace_without_drop(tgt_engine, tgt_schema, table, df)
            print(f"✅ Sincronizado {tgt_schema}.{table} ({len(df)} filas)")

    def _create_union_view_with_estado(self, engine, view_schema: str, view_name: str, raw_schema: str, tables: list, estados: list):
        """Create or replace a view that UNION ALL the provided tables and adds a literal column estado.
        Assumes all tables share identical columns (already validated)."""
        if len(tables) != len(estados):
            raise ValueError("tables and estados must have the same length")
        # Use columns from the first table in target raw schema
        cols = self._get_table_columns(engine, raw_schema, tables[0])
        cols_csv = ", ".join([f'"{c}"' for c in cols])
        selects = []
        for t, est in zip(tables, estados):
            selects.append(f"SELECT {cols_csv}, '{est}'::text AS estado FROM \"{raw_schema}\".\"{t}\"")
        view_sql = f"CREATE SCHEMA IF NOT EXISTS {view_schema};\nCREATE OR REPLACE VIEW \"{view_schema}\".\"{view_name}\" AS\n" + "\nUNION ALL\n".join(selects) + ";"
        with engine.connect() as conn:
            conn.execute(text(view_sql))
        print(f"🧱 Vista {view_schema}.{view_name} creada/actualizada")

    def etl_process(self):
        dataframes_dict = {}

        source_url = self.data_access['sql_url']
        target_url = self.data_access['local_sql_url']

        source_schema = 'banorte_lake'
        source_tables_credito = ['credito_cerrado', 'credito_corriente']
        source_tables_debito = ['debito_cerrado', 'debito_corriente']
        conceptos_master_file = os.path.join(self.strategy_folder, 'Conceptos.xlsx')
        if not os.path.exists(conceptos_master_file):
            df_conceptos_columnas = pd.DataFrame({
                'beneficiario': [None] * 5,
                'categoria': ['Pensiones', 'Reembolsos Eseotres', 'Reembolsos Bombón', 'Casa', 'Ocio', 'Salud'][:5],  # Adjusted to 5 items
                'grupo': ['Gasto fijo', 'No clasificado', 'Vivienda'] + [None] * 2,
                'clave_presupuestal': [None] * 5,
                'concepto_procesado': [None] * 5
            })
            df_conceptos_columnas.to_excel(conceptos_master_file, index=False)

        df_conceptos = pd.read_excel(conceptos_master_file)
        dataframes_dict['conceptos'] = df_conceptos
        #print(df_conceptos.head())

        
        print(f"Folder de trabajo {self.strategy_folder}")
        print(f"Conectando a la fuente de datos: {source_url}")
        print(f"Conectando al destino de datos: {target_url}")

        # Crear motores de conexión (source y, si quieres, target para futuras cargas)
        try:
            src_engine = create_engine(source_url, pool_pre_ping=True)
            with src_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Conexión a la fuente exitosa.")
        except Exception as e:
            print(f"❌ Error conectando a la fuente: {e}")
            return

        # Conectar a destino
        try:
            tgt_engine = create_engine(target_url, pool_pre_ping=True, isolation_level="AUTOCOMMIT")
            with tgt_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Conexión al destino exitosa.")
        except Exception as e:
            print(f"❌ Error conectando al destino: {e}")
            return
        df_credito_corriente = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."credito_corriente"'), src_engine)
        df_credito_cerrado = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."credito_cerrado"'), src_engine)
        df_debito_corriente = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."debito_corriente"'), src_engine)
        df_debito_cerrado = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."debito_cerrado"'), src_engine)
        dataframes_dict['credito_corriente'] = df_credito_corriente
        dataframes_dict['credito_cerrado'] = df_credito_cerrado
        dataframes_dict['debito_corriente'] = df_debito_corriente
        dataframes_dict['debito_cerrado'] = df_debito_cerrado
        conceptos_true = False
        conceptos_true = self.conceptos.generador_de_conceptos(dataframes_dict)
        if conceptos_true: 
            print("✅ Proceso de generación de conceptos finalizado exitosamente. De nuevo a ETL para cargar a SQL.")
        df_conceptos_credito = pd.read_pickle(self.mirror_credito_path)
        df_conceptos_debito = pd.read_pickle(self.mirror_debito_path)

        # Upload to SQL
        try:
            print("⬆️  Subiendo df_conceptos_credito a SQL...")
            df_conceptos_credito.to_sql('credito_con_conceptos', src_engine, schema=source_schema, if_exists='replace', index=False)
            print(f"✅ Subido df_conceptos_credito ({len(df_conceptos_credito)} filas) a {source_schema}.credito_con_conceptos")
            
            print("⬆️  Subiendo df_conceptos_debito a SQL...")
            df_conceptos_debito.to_sql('debito_con_conceptos', src_engine, schema=source_schema, if_exists='replace', index=False)
            print(f"✅ Subido df_conceptos_debito ({len(df_conceptos_debito)} filas) a {source_schema}.debito_con_conceptos")
        except Exception as e:
            print(f"❌ Error subiendo DataFrames a SQL: {e}")
        
        self.union_source_conceptos_SQL()
        
        sql_files = [f for f in os.listdir(self.queries_folder) if f.endswith('.sql')]
        for file in sql_files:
            self.print_query_results(src_engine, file)
            print(f"Encontramos consulta desde archivo {file}...")

    def union_source_conceptos_SQL(self):
        """
        Batch-apply the UNION logic to build/update:
        - banorte_conceptos.credito_corriente   (from banorte_lake.credito_corriente + credito_con_conceptos)
        - banorte_conceptos.credito_cerrado     (from banorte_lake.credito_cerrado   + credito_con_conceptos)
        - banorte_conceptos.debito_corriente    (from banorte_lake.debito_corriente  + debito_con_conceptos)
        - banorte_conceptos.debito_cerrado      (from banorte_lake.debito_cerrado    + debito_con_conceptos)

        IMPORTANT:
        - Uses composite key (k_fecha, k_abono, k_cargo, k_concepto_numbers) where k_concepto_numbers extracts only digits from concepto.
        - Adapts amount column names (abono/cargo vs abonos/cargos).
        - **Handles optional `tarjeta` column** (present in crédito, absent in débito) by selecting `NULL::text AS tarjeta` when missing.
        """
        source_schema = 'banorte_lake'
        target_schema = 'banorte_conceptos'

        # (kind, state, abono_col, cargo_col)
        tasks = [
            ('credito', 'corriente', 'abono',  'cargo'),
            ('credito', 'cerrado',   'abono',  'cargo'),
            ('debito',  'corriente', 'abonos', 'cargos'),
            ('debito',  'cerrado',   'abonos', 'cargos'),
        ]

        # Helper to check if a column exists for a relation
        def _col_exists(engine, schema: str, table: str, column: str) -> bool:
            q = text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name   = :table
                AND column_name  = :column
                LIMIT 1
                """
            )
            with engine.connect() as conn:
                row = conn.execute(q, {"schema": schema, "table": table, "column": column}).fetchone()
            return row is not None

        engine = create_engine(self.data_access['sql_url'], pool_pre_ping=True, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {target_schema}'))

            for kind, state, ab_col, cg_col in tasks:
                base_table      = f'{kind}_{state}'
                conceptos_table = f'{kind}_con_conceptos'
                target_table    = f'{target_schema}.{base_table}'

                # Determine how to project `tarjeta` for each source; alias to `tarjeta` in the CTEs
                base_has_tarjeta = _col_exists(engine, source_schema, base_table, 'tarjeta')
                cc_has_tarjeta   = _col_exists(engine, source_schema, conceptos_table, 'tarjeta')

                c_tarjeta_sel  = 'tarjeta' if base_has_tarjeta else 'NULL::text AS tarjeta'
                cc_tarjeta_sel = 'tarjeta' if cc_has_tarjeta   else 'NULL::text AS tarjeta'

                sql = f"""
                BEGIN;

                DROP TABLE IF EXISTS {target_table};

                CREATE TABLE {target_table} AS
                WITH
                -- Base table with normalized key
                c AS (
                    SELECT
                        fecha,
                        concepto,
                        {ab_col},
                        {cg_col},
                        {c_tarjeta_sel},
                        file_date,
                        file_name,
                        -- keys (normalized)
                        date(fecha)                          AS k_fecha,
                        COALESCE({ab_col},0)::numeric(18,2)  AS k_abono,
                        COALESCE({cg_col},0)::numeric(18,2)  AS k_cargo,
                        regexp_replace(concepto, '[^0-9]', '', 'g') AS k_concepto_numbers
                    FROM {source_schema}.{base_table}
                ),

                -- Enrichment table normalized and de-duplicated on the same key
                cc_dedup AS (
                SELECT *
                FROM (
                    SELECT
                        fecha,
                        concepto,
                        {ab_col},
                        {cg_col},
                        {cc_tarjeta_sel},
                        file_date,
                        file_name,
                        beneficiario,
                        categoria,
                        grupo,
                        clave_presupuestal,
                        concepto_procesado,
                        -- keys (normalized)
                        date(fecha)                          AS k_fecha,
                        COALESCE({ab_col},0)::numeric(18,2)  AS k_abono,
                        COALESCE({cg_col},0)::numeric(18,2)  AS k_cargo,
                        regexp_replace(concepto, '[^0-9]', '', 'g') AS k_concepto_numbers,
                        ROW_NUMBER() OVER (
                            PARTITION BY date(fecha),
                                        COALESCE({ab_col},0)::numeric(18,2),
                                        COALESCE({cg_col},0)::numeric(18,2),
                                        regexp_replace(concepto, '[^0-9]', '', 'g')
                            ORDER BY
                                (CASE WHEN categoria     IS NOT NULL THEN 0 ELSE 1 END),
                                (CASE WHEN beneficiario  IS NOT NULL THEN 0 ELSE 1 END),
                                file_date DESC NULLS LAST,
                                fecha     DESC NULLS LAST
                        ) AS rn
                    FROM {source_schema}.{conceptos_table}
                ) t
                WHERE rn = 1
                )

                -- Build the mirror using (k_fecha, k_abono, k_cargo, k_concepto_numbers)
                SELECT
                    c.fecha,
                    c.concepto,
                    c.{ab_col},
                    c.{cg_col},
                    c.tarjeta,
                    c.file_date,
                    c.file_name,
                    cc.beneficiario,
                    cc.categoria,
                    cc.grupo,
                    cc.clave_presupuestal,
                    cc.concepto_procesado
                FROM c
                LEFT JOIN cc_dedup cc
                ON  c.k_fecha IS NOT DISTINCT FROM cc.k_fecha
                AND c.k_abono IS NOT DISTINCT FROM cc.k_abono
                AND c.k_cargo IS NOT DISTINCT FROM cc.k_cargo
                AND c.k_concepto_numbers IS NOT DISTINCT FROM cc.k_concepto_numbers
                ;

                -- Helpful indexes
                CREATE INDEX ON {target_table} (file_date);
                CREATE INDEX ON {target_table} (file_name);
                CREATE INDEX ON {target_table} (categoria);

                COMMIT;
                """
                conn.execute(text(sql))
                print(f"🧱 Tabla {target_table} creada/actualizada.")


    def print_query_results(self, engine, sql_file):
        """Execute the query and print the output in terminal. The files are already tested, so we only need to run them."""
        file_path = os.path.join(self.queries_folder, sql_file)
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Split the file into individual queries based on comment lines starting new queries
        queries = []
        current_query = []
        for line in content.split('\n'):
            if line.strip().startswith('--') and 'New query for' in line:
                if current_query:
                    queries.append('\n'.join(current_query).strip())
                    current_query = []
            current_query.append(line)
        if current_query:
            queries.append('\n'.join(current_query).strip())
        
        # Remove empty queries
        queries = [q for q in queries if q.strip()]
        
        for i, query in enumerate(queries):
            try:
                df = pd.read_sql(text(query), engine)
                # Replace NaN/None with empty string
                df = df.fillna('')
                # Format numeric columns as currency ($ with commas)
                formatters = {}
                for col in df.columns:
                    if df[col].dtype in ['float64', 'int64']:
                        formatters[col] = lambda x: f"${x:,.2f}" if x != '' else ''
                    else:
                        formatters[col] = str
                
                # Determine friendly header based on table name in query
                if 'credito_corriente' in query:
                    header = "📊 Totals for Crédito Corriente (all data):"
                elif 'credito_cerrado' in query:
                    header = "📊 Totals for Crédito Cerrado (filtered to newest file_date):"
                elif 'debito_corriente' in query:
                    header = "📊 Totals for Débito Corriente (all data):"
                elif 'debito_cerrado' in query:
                    header = "📊 Totals for Débito Cerrado (filtered to newest file_date):"
                else:
                    header = f"📊 Results for {sql_file} (Query {i+1}):"
                
                print(f"\n{header}")
                print(df.to_string(index=False, formatters=formatters))
            except Exception as e:
                print(f"❌ Error executing query {i+1} from {sql_file}: {e}")



def main():
    folder_root = os.getcwd()
    strategy_folder = os.path.join(folder_root, "Implementación", "Estrategia")
    passwords_path = os.path.join(folder_root, "Implementación", "Info Bancaria", 'passwords.yaml')
    with open(passwords_path, 'r') as f:
        data_access = yaml.safe_load(f)
    DataWarehouse(strategy_folder, data_access).etl_process()

if __name__ == "__main__":
    main()
