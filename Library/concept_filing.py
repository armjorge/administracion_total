import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import yaml
from urllib.parse import urlparse

class CONCEPT_FILING:

    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access 

    def run_streamlit_interface(self):
        # 1) Parse DB URL from self.data_access['sql_workflow']
        sql_url = self.data_access['sql_workflow']
        parsed = urlparse(sql_url)
        dbname = parsed.path.lstrip('/')
        user = parsed.username
        password = parsed.password
        host = parsed.hostname
        port = parsed.port

        # 2) Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        # 3) Streamlit UI
        st.set_page_config(page_title="Banorte Conceptos", layout="wide")

        # Añadimos la nueva vista
        vista = st.sidebar.radio(
            "Seleccionar vista:",
            [
                "Clasificador de conceptos",
                "Catálogo de categorías",
                "Catálogo de beneficiarios",
                "Catálogo de cuentas"
            ]
        )

        schema = "banorte_load"

        # ========== CLASIFICADOR DE CONCEPTOS ==========
        if vista == "Clasificador de conceptos":
            st.title("🏦 Clasificador de Conceptos Banorte")

            tipo = st.sidebar.radio("Seleccionar tipo de movimientos:", ["Débito", "Crédito"])
            table_name = "debito_conceptos" if tipo == "Débito" else "credito_conceptos"

            # =====================================================
            # 1) Cargar datos principales
            # =====================================================
            try:
                query = f"""
                SELECT fecha, unique_concept, concepto, cuenta, estado,
                    cargo, abono, category_group, category_subgroup, beneficiario
                FROM "{schema}"."{table_name}"
                ORDER BY fecha DESC
                LIMIT 1000;
                """
                df_raw = pd.read_sql(query, conn)
            except:
                st.error("No se pudo cargar la tabla.")
                return

            if df_raw.empty:
                st.info("No hay registros disponibles.")
                return

            # =====================================================
            # 2) Cargar catálogos
            # =====================================================
            try:
                df_cat = pd.read_sql(f'SELECT DISTINCT "group", subgroup FROM "{schema}".category;', conn)
            except:
                df_cat = pd.DataFrame(columns=["group", "subgroup"])

            try:
                df_benef = pd.read_sql(f'SELECT nombre FROM "{schema}".beneficiaries;', conn)
            except:
                df_benef = pd.DataFrame(columns=["nombre"])

            # =====================================================
            # 3) FILTROS dinamicos
            # =====================================================
            st.sidebar.markdown("### Filtrar datos")

            # --- Filtro estado ---
            estados = ["Todos"] + sorted(df_raw["estado"].dropna().unique().tolist())
            estado_sel = st.sidebar.selectbox("Estado del movimiento", estados)

            # --- Filtro cuenta ---
            cuentas = ["Todas"] + sorted(df_raw["cuenta"].dropna().unique().tolist())
            cuenta_sel = st.sidebar.selectbox("Cuenta", cuentas)

            # --- Aplicar filtros ---
            df = df_raw.copy()

            if estado_sel != "Todos":
                df = df[df["estado"] == estado_sel]

            if cuenta_sel != "Todas":
                df = df[df["cuenta"] == cuenta_sel]

            # =====================================================
            # 4) Mostrar tabla filtrada
            # =====================================================
            st.markdown("### Registros filtrados")
            if df.empty:
                st.warning("No hay registros con esos filtros.")
                return

            # 🔥 La línea que corrige el problema:
            df = df.reset_index(drop=True)

            st.dataframe(df, use_container_width=True, height=400)

            st.markdown("---")
            st.markdown("### ✏️ Actualizar clasificación")

            # =====================================================
            # 5) Selección de índice basado en la TABLA FILTRADA
            # =====================================================
            selected_index = st.number_input(
                "Selecciona el índice de fila a editar",
                min_value=0,
                max_value=len(df)-1,
                value=0,
                step=1
            )
            selected_row = df.iloc[selected_index]

            st.write(f"**Concepto:** {selected_row['concepto']} ({selected_row['unique_concept']})")

            st.markdown(
                f"""
                **Fecha:** {selected_row['fecha']}  
                **Abono:** {selected_row['abono']}  
                **Cargo:** {selected_row['cargo']}  
                **Cuenta:** {selected_row.get('cuenta', '—')}
                """,
                unsafe_allow_html=True
            )

            # =====================================================
            # 6) Selectboxes del catálogo
            # =====================================================
            grupos = sorted(df_cat['group'].dropna().unique().tolist())
            current_group = selected_row.get("category_group", None)
            group_idx = grupos.index(current_group) if current_group in grupos else 0

            group = st.selectbox("Grupo", options=grupos, index=group_idx)

            subgrupos_filtrados = sorted(
                df_cat[df_cat['group'] == group]['subgroup'].dropna().unique().tolist()
            )
            current_subgroup = selected_row.get("category_subgroup", None)
            subgroup_idx = subgrupos_filtrados.index(current_subgroup) if current_subgroup in subgrupos_filtrados else 0

            subgroup = st.selectbox("Subgrupo", options=subgrupos_filtrados, index=subgroup_idx)

            beneficiarios = [''] + sorted(df_benef['nombre'].dropna().unique().tolist())
            current_benef = selected_row.get("beneficiario", "")
            benef_idx = beneficiarios.index(current_benef) if current_benef in beneficiarios else 0

            benef = st.selectbox("Beneficiario", options=beneficiarios, index=benef_idx)

            # =====================================================
            # 7) Guardar cambios
            # =====================================================
            if st.button("💾 Guardar cambios"):
                if not group or not subgroup:
                    st.warning("⚠️ Debes seleccionar un grupo y un subgrupo.")
                else:
                    with conn.cursor() as cur:
                        update_query = f'''
                        UPDATE "{schema}"."{table_name}"
                        SET category_group = %s,
                            category_subgroup = %s,
                            beneficiario = %s
                        WHERE fecha = %s AND unique_concept = %s;
                        '''
                        cur.execute(
                            update_query,
                            (group, subgroup, benef, selected_row['fecha'], selected_row['unique_concept'])
                        )
                        conn.commit()
                    st.success("✅ Clasificación actualizada correctamente.")

        # ========== CATÁLOGO DE CATEGORÍAS ==========
        elif vista == "Catálogo de categorías":
            st.title("📘 Catálogo de Categorías")
            try:
                df_cat = pd.read_sql(f'SELECT * FROM "{schema}".category ORDER BY "group", subgroup;', conn)
            except Exception:
                df_cat = pd.DataFrame(columns=["group", "subgroup"])
            st.dataframe(df_cat, use_container_width=True)
            st.markdown("### ➕ Agregar nueva categoría")
            new_group = st.text_input("Group")
            new_subgroup = st.text_input("Subgroup")
            if st.button("Agregar categoría"):
                if new_group and new_subgroup:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'INSERT INTO "{schema}".category ("group", subgroup) VALUES (%s, %s) ON CONFLICT DO NOTHING;',
                            (new_group, new_subgroup)
                        )
                        conn.commit()
                    st.success("✅ Categoría agregada correctamente.")
                else:
                    st.warning("⚠️ Ingresa ambos campos.")

        # ========== CATÁLOGO DE BENEFICIARIOS ==========
        elif vista == "Catálogo de beneficiarios":
            st.title("📗 Catálogo de Beneficiarios")
            try:
                df_benef = pd.read_sql(f'SELECT * FROM "{schema}".beneficiaries ORDER BY nombre;', conn)
            except Exception:
                df_benef = pd.DataFrame(columns=["nombre"])
            st.dataframe(df_benef, use_container_width=True)
            st.markdown("### ➕ Agregar nuevo beneficiario")
            new_benef = st.text_input("Nombre del beneficiario")
            if st.button("Agregar beneficiario"):
                if new_benef:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'INSERT INTO "{schema}".beneficiaries (nombre) VALUES (%s) ON CONFLICT DO NOTHING;',
                            (new_benef,)
                        )
                        conn.commit()
                    st.success("✅ Beneficiario agregado correctamente.")
                else:
                    st.warning("⚠️ El nombre no puede estar vacío.")

        # ========== CATÁLOGO DE CUENTAS ==========
        elif vista == "Catálogo de cuentas":
            st.title("💳 Catálogo de Cuentas")
            try:
                df_accounts = pd.read_sql(
                    f'SELECT * FROM "{schema}".accounts ORDER BY account_number;',
                    conn
                )
            except Exception:
                df_accounts = pd.DataFrame(columns=["account_number", "type"])

            st.dataframe(df_accounts, use_container_width=True)
            st.markdown("### ➕ Agregar nueva cuenta")

            new_account = st.text_input("Número de cuenta")
            new_type = st.selectbox("Tipo", ["debit", "credit"])

            if st.button("Agregar cuenta"):
                if new_account:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'INSERT INTO "{schema}".accounts (account_number, type) VALUES (%s, %s) ON CONFLICT DO NOTHING;',
                            (new_account, new_type)
                        )
                        conn.commit()
                    st.success("✅ Cuenta agregada correctamente.")
                else:
                    st.warning("⚠️ El número de cuenta no puede estar vacío.")
    def sql_conexion(self, sql_url):
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None
        
if __name__ == "__main__":
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    folder_name = "MAIN_PATH"
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        working_folder = os.getenv(folder_name)    
    yaml_path = os.path.join(working_folder, 'config.yaml')
    with open(yaml_path, 'r') as file:
        data_access = yaml.safe_load(file)
    app = CONCEPT_FILING(working_folder, data_access)
    app.run_streamlit_interface()