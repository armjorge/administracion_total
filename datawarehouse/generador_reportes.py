import os
import datetime as dt
from typing import Optional

import yaml
import pandas as pd
from sqlalchemy import create_engine, text


EXCLUDED_SCHEMAS = {"pg_toast", "pg_temp_1", "pg_toast_temp_1", "pg_catalog", "information_schema"}


class GeneradorReportes:
    def __init__(self, data_access: dict, strategy_folder: str):
        self.data_access = data_access
        self.strategy_folder = strategy_folder
        self.engine = create_engine(self.data_access["local_sql_url"], pool_pre_ping=True)

    def _out_dir(self, subfolder: str = "reports") -> str:
        day = dt.date.today().isoformat()
        path = os.path.join(self.strategy_folder, subfolder, day)
        os.makedirs(path, exist_ok=True)
        return path

    def schemas(self) -> pd.DataFrame:
        q = text(
            """
            SELECT n.nspname AS schema
            FROM pg_namespace n
            WHERE n.nspname NOT IN :excluded
            ORDER BY 1
            """
        )
        with self.engine.connect() as conn:
            df = pd.read_sql(q, conn, params={"excluded": tuple(EXCLUDED_SCHEMAS)})
        return df

    def tables(self) -> pd.DataFrame:
        q = text(
            """
            SELECT table_schema AS schema, table_name AS name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE' AND table_schema NOT IN :excluded
            ORDER BY 1,2
            """
        )
        with self.engine.connect() as conn:
            return pd.read_sql(q, conn, params={"excluded": tuple(EXCLUDED_SCHEMAS)})

    def views(self) -> pd.DataFrame:
        q = text(
            """
            SELECT table_schema AS schema, table_name AS name
            FROM information_schema.views
            WHERE table_schema NOT IN :excluded
            ORDER BY 1,2
            """
        )
        with self.engine.connect() as conn:
            return pd.read_sql(q, conn, params={"excluded": tuple(EXCLUDED_SCHEMAS)})

    def matviews(self) -> pd.DataFrame:
        q = text(
            """
            SELECT schemaname AS schema, matviewname AS name
            FROM pg_matviews
            WHERE schemaname NOT IN :excluded
            ORDER BY 1,2
            """
        )
        with self.engine.connect() as conn:
            return pd.read_sql(q, conn, params={"excluded": tuple(EXCLUDED_SCHEMAS)})

    def rowcounts(self) -> pd.DataFrame:
        q = text(
            """
            SELECT n.nspname AS schema,
                   c.relname AS name,
                   CASE WHEN c.relkind IN ('r','m') THEN 'table' ELSE c.relkind::text END AS kind,
                   pg_total_relation_size(c.oid) AS bytes_total,
                   COALESCE(c.reltuples, 0) AS rows_estimated
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN :excluded
              AND c.relkind IN ('r','m') -- r=table, m=matview
            ORDER BY 1,2
            """
        )
        with self.engine.connect() as conn:
            return pd.read_sql(q, conn, params={"excluded": tuple(EXCLUDED_SCHEMAS)})

    def catalog_summary(self) -> pd.DataFrame:
        t = self.tables()
        v = self.views()
        m = self.matviews()
        s = self.schemas()
        return pd.DataFrame({
            "schemas": [len(s)],
            "tables": [len(t)],
            "views": [len(v)],
            "matviews": [len(m)],
        })

    def export_catalog(self, fmt: str = "html") -> str:
        out = self._out_dir("reports")
        # Write individual CSVs always for convenience
        self.schemas().to_csv(os.path.join(out, "schemas.csv"), index=False)
        self.tables().to_csv(os.path.join(out, "tables.csv"), index=False)
        self.views().to_csv(os.path.join(out, "views.csv"), index=False)
        self.matviews().to_csv(os.path.join(out, "matviews.csv"), index=False)
        self.rowcounts().to_csv(os.path.join(out, "rowcounts.csv"), index=False)
        self.catalog_summary().to_csv(os.path.join(out, "summary.csv"), index=False)

        # Combined HTML/MD snapshot
        if fmt == "html":
            html_path = os.path.join(out, "catalogo.html")
            parts = []
            parts.append("<h2>Resumen</h2>" + self.catalog_summary().to_html(index=False))
            parts.append("<h3>Schemas</h3>" + self.schemas().to_html(index=False))
            parts.append("<h3>Tables</h3>" + self.tables().to_html(index=False))
            parts.append("<h3>Views</h3>" + self.views().to_html(index=False))
            parts.append("<h3>Materialized Views</h3>" + self.matviews().to_html(index=False))
            parts.append("<h3>Rowcounts (estimados) y tamaño</h3>" + self.rowcounts().to_html(index=False))
            with open(html_path, "w") as f:
                f.write("\n".join(parts))
            return html_path
        elif fmt == "md":
            md_path = os.path.join(out, "catalogo.md")
            with open(md_path, "w") as f:
                f.write("# Resumen\n\n")
                f.write(self.catalog_summary().to_markdown(index=False) + "\n\n")
                f.write("## Schemas\n\n" + self.schemas().to_markdown(index=False) + "\n\n")
                f.write("## Tables\n\n" + self.tables().to_markdown(index=False) + "\n\n")
                f.write("## Views\n\n" + self.views().to_markdown(index=False) + "\n\n")
                f.write("## Materialized Views\n\n" + self.matviews().to_markdown(index=False) + "\n\n")
                f.write("## Rowcounts (estimados) y tamaño\n\n" + self.rowcounts().to_markdown(index=False) + "\n")
            return md_path
        else:
            raise ValueError("fmt debe ser 'html' o 'md'")

    def generar_reporte_catalogo(self, fmt: str = "html") -> str:
        print("Generando reporte de catálogo de la base local…")
        path = self.export_catalog(fmt=fmt)
        print(f"✅ Reporte generado: {path}")
        return path


def main(fmt: Optional[str] = None):
    folder_root = os.getcwd()
    strategy_folder = os.path.join(folder_root, "Implementación", "Estrategia")
    passwords_path = os.path.join(folder_root, "Implementación", "Info Bancaria", 'passwords.yaml')
    with open(passwords_path, 'r') as f:
        data_access = yaml.safe_load(f)
    fmt_use = fmt or "html"
    GeneradorReportes(data_access, strategy_folder).generar_reporte_catalogo(fmt=fmt_use)


if __name__ == "__main__":
    main()
