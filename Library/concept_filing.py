import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os

import yaml


class CONCEPT_FILING:

    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access 

    def run_streamlit_interface(self):
        import streamlit as st
        import pandas as pd
        import psycopg2
        from urllib.parse import urlparse

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
        # Nuevo selector de vista
        vista = st.sidebar.radio("Seleccionar vista:", ["Clasificador de conceptos", "Catálogo de categorías", "Catálogo de beneficiarios"])

        schema = "banorte_load"

        if vista == "Clasificador de conceptos":
            st.title("🏦 Clasificador de Conceptos Banorte")
            tipo = st.sidebar.radio("Seleccionar tipo de movimientos:", ["Débito", "Crédito"])

            table_name = "debito_conceptos" if tipo == "Débito" else "credito_conceptos"

            # 4) Fetch data (usar columnas reales y llaves existentes)
            query = f"""
            SELECT fecha, unique_concept, concepto, cuenta, estado,
                   cargo, abono, category_group, category_subgroup, beneficiario
            FROM "{schema}"."{table_name}"
            ORDER BY fecha DESC
            LIMIT 100;
            """
            df = pd.read_sql(query, conn)

            # 5) Tablas de referencia reales
            df_cat = pd.read_sql(f'SELECT DISTINCT "group", subgroup FROM "{schema}".category;', conn)
            df_benef = pd.read_sql(f'SELECT nombre FROM "{schema}".beneficiarios;', conn)

            # 6) UI principal
            st.markdown("### Registros disponibles")
            st.dataframe(df, use_container_width=True, height=400)

            st.markdown("---")
            st.markdown("### ✏️ Actualizar clasificación")

            if df.empty:
                st.warning("No hay registros para editar.")
                return

            selected_index = st.number_input(
                "Selecciona el índice de fila a editar",
                min_value=0, max_value=len(df)-1, value=0, step=1
            )
            selected_row = df.iloc[selected_index]

            st.write(f"**Concepto:** {selected_row['concepto']} ({selected_row['unique_concept']})")

            # Mostrar detalles adicionales del registro seleccionado
            st.markdown(
                f"""
                **Fecha:** {selected_row['fecha']}  
                **Abono:** {selected_row['abono']}  
                **Cargo:** {selected_row['cargo']}  
                **Cuenta:** {selected_row.get('cuenta', '—')}
                """,
                unsafe_allow_html=True
            )

            # Opciones de grupo y subgrupo (category)
            grupos = sorted(df_cat['group'].dropna().unique().tolist())
            current_group = selected_row.get("category_group", None)
            group = st.selectbox("Grupo", options=grupos,
                                 index=grupos.index(current_group) if current_group in grupos else 0)

            subgrupos_filtrados = sorted(
                df_cat[df_cat['group'] == group]['subgroup'].dropna().unique().tolist()
            )
            current_subgroup = selected_row.get("category_subgroup", None)
            subgroup = st.selectbox("Subgrupo", options=subgrupos_filtrados,
                                    index=subgrupos_filtrados.index(current_subgroup)
                                    if current_subgroup in subgrupos_filtrados else 0)

            # Beneficiarios
            beneficiarios = [''] + sorted(df_benef['nombre'].dropna().unique().tolist())
            current_benef = selected_row.get("beneficiario", "")
            benef = st.selectbox("Beneficiario", options=beneficiarios,
                                 index=beneficiarios.index(current_benef)
                                 if current_benef in beneficiarios else 0)

            # 7) Guardar cambios
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

        elif vista == "Catálogo de categorías":
            st.title("📘 Catálogo de Categorías")
            df_cat = pd.read_sql(f'SELECT * FROM "{schema}".category ORDER BY "group", subgroup;', conn)
            st.dataframe(df_cat, use_container_width=True)
            st.markdown("### ➕ Agregar nueva categoría")
            new_group = st.text_input("Group")
            new_subgroup = st.text_input("Subgroup")
            if st.button("Agregar categoría"):
                if new_group and new_subgroup:
                    with conn.cursor() as cur:
                        cur.execute(f'INSERT INTO "{schema}".category ("group", subgroup) VALUES (%s, %s) ON CONFLICT DO NOTHING;', (new_group, new_subgroup))
                        conn.commit()
                    st.success("✅ Categoría agregada correctamente.")
                else:
                    st.warning("⚠️ Ingresa ambos campos.")

        elif vista == "Catálogo de beneficiarios":
            st.title("📗 Catálogo de Beneficiarios")
            df_benef = pd.read_sql(f'SELECT * FROM "{schema}".beneficiarios ORDER BY nombre;', conn)
            st.dataframe(df_benef, use_container_width=True)
            st.markdown("### ➕ Agregar nuevo beneficiario")
            new_benef = st.text_input("Nombre del beneficiario")
            if st.button("Agregar beneficiario"):
                if new_benef:
                    with conn.cursor() as cur:
                        cur.execute(f'INSERT INTO "{schema}".beneficiarios (nombre) VALUES (%s) ON CONFLICT DO NOTHING;', (new_benef,))
                        conn.commit()
                    st.success("✅ Beneficiario agregado correctamente.")
                else:
                    st.warning("⚠️ El nombre no puede estar vacío.")

    def sql_conexion(self, sql_url):
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None
        
if __name__ == "__main__":
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    with open(env_path, 'r') as file:
        env_data = yaml.safe_load(file)
    working_folder = env_data['Main_path']
    yaml_path = os.path.join(working_folder, 'config.yaml')
    with open(yaml_path, 'r') as file:
        data_access = yaml.safe_load(file)
    app = CONCEPT_FILING(working_folder, data_access)
    app.run_streamlit_interface()