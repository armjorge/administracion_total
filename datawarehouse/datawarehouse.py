import os  # Import the os module
#from utils.helpers import Helper  # Import the Helper class
import yaml
from sqlalchemy import create_engine, text
import pandas as pd
from generador_reportes import GeneradorReportes

class DataWarehouse:
    def __init__(self, strategy_folder, data_access):
        self.strategy_folder = strategy_folder
        self.data_access = data_access
        #self.helper = Helper()
        self.generador_reportes = GeneradorReportes(self.data_access, self.strategy_folder)

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
        source_url = self.data_access['sql_url']
        target_url = self.data_access['local_sql_url']

        source_schema = 'banorte_lake'
        source_tables_credito = ['credito_cerrado', 'credito_corriente']
        source_tables_debito = ['debito_cerrado', 'debito_corriente']
        
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

        # Esquemas destino
        raw_schema_tgt = 'raw_banorte'
        stage_schema_tgt = 'stage_banorte'
        self._ensure_schema(tgt_engine, raw_schema_tgt)
        self._ensure_schema(tgt_engine, stage_schema_tgt)

        # Copiar/sincronizar tablas fuente -> destino (raw) según reglas de actualización
        for t in (source_tables_credito + source_tables_debito):
            self._sync_table(src_engine, tgt_engine, source_schema, t, raw_schema_tgt)

        # Crear vistas unificadas por grupo con columna estado
        self._create_union_view_with_estado(
            tgt_engine,
            view_schema=stage_schema_tgt,
            view_name='credito_all',
            raw_schema=raw_schema_tgt,
            tables=['credito_cerrado', 'credito_corriente'],
            estados=['cerrado', 'corriente']
        )
        self._create_union_view_with_estado(
            tgt_engine,
            view_schema=stage_schema_tgt,
            view_name='debito_all',
            raw_schema=raw_schema_tgt,
            tables=['debito_cerrado', 'debito_corriente'],
            estados=['cerrado', 'corriente']
        )

        # Comparar columnas por grupos
        self._compare_table_groups(src_engine, source_schema, source_tables_credito, group_name="CRÉDITO")
        self._compare_table_groups(src_engine, source_schema, source_tables_debito, group_name="DÉBITO")

        # Construir/actualizar DW y marts
        self.build_dw()
        self.generador_reportes.generar_reporte_catalogo()

    def build_dw(self):
        """Create/refresh DW: fact table + indexes + marts (materialized views)."""
        target_url = self.data_access['local_sql_url']
        engine = create_engine(target_url, pool_pre_ping=True, isolation_level="AUTOCOMMIT")
        ddl = text(
            """
            CREATE SCHEMA IF NOT EXISTS dw_banorte;
            CREATE TABLE IF NOT EXISTS dw_banorte.movimientos (
              id BIGSERIAL PRIMARY KEY,
              tipo TEXT NOT NULL,
              estado TEXT NOT NULL,
              fecha DATE NOT NULL,
              concepto TEXT,
              abono NUMERIC(14,2),
              cargo NUMERIC(14,2),
              tarjeta TEXT,
              file_date DATE,
              file_name TEXT
            );
            -- Reemplazo seguro sin DROP
            TRUNCATE TABLE dw_banorte.movimientos;
            INSERT INTO dw_banorte.movimientos (tipo, estado, fecha, concepto, abono, cargo, tarjeta, file_date, file_name)
            SELECT 'credito' AS tipo,
                   estado,
                   fecha::date,
                   concepto,
                   abono::numeric,
                   cargo::numeric,
                   tarjeta,
                   file_date::date,
                   file_name
            FROM stage_banorte.credito_all
            UNION ALL
            SELECT 'debito'  AS tipo,
                   estado,
                   fecha::date,
                   concepto,
                   abonos::numeric     AS abono,
                   cargos::numeric     AS cargo,
                   NULL::text          AS tarjeta,
                   file_date::date,
                   file_name
            FROM stage_banorte.debito_all;

            CREATE INDEX IF NOT EXISTS idx_dw_mov_fecha       ON dw_banorte.movimientos (fecha);
            CREATE INDEX IF NOT EXISTS idx_dw_mov_tipo_estado ON dw_banorte.movimientos (tipo, estado);
            CREATE INDEX IF NOT EXISTS idx_dw_mov_file_date   ON dw_banorte.movimientos (file_date);

            -- Materialized views
            CREATE MATERIALIZED VIEW IF NOT EXISTS dw_banorte.mv_diario_tipo AS
            SELECT fecha, tipo,
                   COALESCE(SUM(abono),0) AS total_abonos,
                   COALESCE(SUM(cargo),0) AS total_cargos
            FROM dw_banorte.movimientos
            GROUP BY 1,2;
            CREATE INDEX IF NOT EXISTS idx_mv_diario_tipo_fecha ON dw_banorte.mv_diario_tipo (fecha);

            CREATE MATERIALIZED VIEW IF NOT EXISTS dw_banorte.mv_mensual_estado AS
            SELECT date_trunc('month', fecha)::date AS mes, tipo, estado,
                   COALESCE(SUM(abono),0) AS total_abonos,
                   COALESCE(SUM(cargo),0) AS total_cargos
            FROM dw_banorte.movimientos
            GROUP BY 1,2,3;
            CREATE INDEX IF NOT EXISTS idx_mv_mensual_estado_mes ON dw_banorte.mv_mensual_estado (mes);
            """
        )
        with engine.connect() as conn:
            conn.execute(ddl)
            # Refresh MVs after load
            conn.execute(text("REFRESH MATERIALIZED VIEW dw_banorte.mv_diario_tipo"))
            conn.execute(text("REFRESH MATERIALIZED VIEW dw_banorte.mv_mensual_estado"))
        print("🏗️  DW construido/refrescado: movimientos + MVs actualizadas")


def main():
    folder_root = os.getcwd()
    strategy_folder = os.path.join(folder_root, "Implementación", "Estrategia")
    passwords_path = os.path.join(folder_root, "Implementación", "Info Bancaria", 'passwords.yaml')
    with open(passwords_path, 'r') as f:
        data_access = yaml.safe_load(f)
    DataWarehouse(strategy_folder, data_access).etl_process()

if __name__ == "__main__":
    main()
