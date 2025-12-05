import sys
import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import date
st.title("Inicializar tus cuentas")

# ================== extraer links de .env ==================
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_PATH not in sys.path:
    sys.path.insert(0, BASE_PATH)

env_file = os.path.join(BASE_PATH, ".env")

# Si no existe el .env, redirigir al usuario a configurar bases de datos
if not os.path.exists(env_file):
    st.warning("Necesitas configurar las bases de datos primero.")
    st.page_link(
        "pages/00_database.py",
        label="Ir a configuración de bases de datos",
        icon="💾",
    )
    st.stop()

# Cargar variables del .env
load_dotenv(dotenv_path=env_file)

postgresql_key = "sql_workflow"
mongo_db_key = "MONGO_URI"

pg_url = os.getenv(postgresql_key, "")
mongo_url = os.getenv(mongo_db_key, "")

# Si faltan claves, avisar y mandar a la página de configuración
if not pg_url or not mongo_url:
    st.error("No se encontraron todas las conexiones necesarias en el `.env`.")
    st.write("Por favor configura tus bases de datos:")
    st.page_link(
        "pages/00_database.py",
        label="Configurar bases de datos",
        icon="💾",
    )
    st.stop()

# Construir data_access con la info del .env
data_access = {
    postgresql_key: pg_url,
    mongo_db_key: mongo_url,
}

# (Opcional) Guardar en session_state para reutilizar en otras páginas
st.session_state["data_access"] = data_access

st.success("Conexiones cargadas correctamente ✅")
# Aquí ya puedes seguir con la lógica de inicializar cuentas usando data_access

# ================== Esquema editable (default: banking) ==================
default_schema = st.session_state.get("schema", "banking")
schema = st.text_input("Esquema de trabajo en PostgreSQL", value=default_schema)
st.session_state["schema"] = schema

st.write(f"Trabajando con el esquema: `{schema}`")

# ================== Conexión a PostgreSQL ==================
from sqlalchemy.exc import SQLAlchemyError

def sql_conexion(sql_url: str):
    try:
        engine = create_engine(sql_url)
        return engine
    except Exception as e:
        st.error(f"❌ Error creando engine de base de datos: {e}")
        return None

engine = sql_conexion(data_access["sql_workflow"])
if engine is None:
    st.stop()

try:
    connexion = engine.connect()
except SQLAlchemyError as e:
    st.error(f"❌ No se pudo establecer conexión con PostgreSQL: {e}")
    st.stop()

# ================== Intentar leer las 4 tablas ==================
st.subheader("Estado de las tablas del esquema")

query_accounts          = f"SELECT * FROM {schema}.accounts"
query_cutoff_days       = f"SELECT * FROM {schema}.cutoff_days"
query_cutoff_years      = f"SELECT * FROM {schema}.cutoff_years"
query_account_cutoffs   = f"SELECT * FROM {schema}.account_cutoffs"

tables_ok = True
df_accounts = df_cutoff_days = df_cutoff_years = df_account_cutoffs = None
error_msg = None

try:
    df_accounts = pd.read_sql(query_accounts, connexion)
    df_cutoff_days = pd.read_sql(query_cutoff_days, connexion)
    df_cutoff_years = pd.read_sql(query_cutoff_years, connexion)
    df_account_cutoffs = pd.read_sql(query_account_cutoffs, connexion)
except Exception as e:
    tables_ok = False
    error_msg = str(e)

if tables_ok:

    st.success("✅ Todas las tablas se encontraron correctamente.")

    # ─────────────────────────────────────────────
    #  Accounts (modificable)  |  account_cutoffs (solo lectura)
    # ─────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    # ==== Lado izquierdo: accounts + formulario para agregar ====
    with col_left:
        with st.expander("Tabla accounts (modificable)", expanded=True):
            st.dataframe(df_accounts, use_container_width=True)

            st.markdown("#### Agregar nueva cuenta")

            new_account_number = st.text_input(
                "Número de cuenta (3 a 5 caracteres, letras o números)",
                max_chars=5,
                key="new_account_number",
            )

            new_account_type = st.selectbox(
                "Tipo de cuenta",
                ["debit", "credit"],
                key="new_account_type",
            )

            if st.button("Guardar cuenta", key="btn_save_account"):
                errores = []

                # Validaciones
                if not new_account_number:
                    errores.append("El número de cuenta no puede estar vacío.")
                elif len(new_account_number) < 3 or len(new_account_number) > 5:
                    errores.append("El número de cuenta debe tener entre 3 y 5 caracteres.")
                elif not new_account_number.isalnum():
                    errores.append("El número de cuenta solo puede contener letras y números (sin caracteres especiales ni emojis).")

                if not new_account_type:
                    errores.append("Debes seleccionar un tipo de cuenta.")

                if errores:
                    for err in errores:
                        st.error(err)
                else:
                    try:
                        insert_sql = text(f"""
                            INSERT INTO {schema}.accounts (account_number, type)
                            VALUES (:acc, :typ)
                            ON CONFLICT (account_number, type) DO NOTHING;
                        """)
                        with engine.begin() as conn:
                            conn.execute(
                                insert_sql,
                                {"acc": new_account_number, "typ": new_account_type},
                            )
                        st.success("Cuenta guardada correctamente ✅")
                        st.rerun()  # recargar datos
                    except Exception as e:
                        st.error(f"Error al guardar la cuenta: {e}")

    # ==== Lado derecho: account_cutoffs + botón de refresco ====
    with col_right:
        with st.expander("Tabla account_cutoffs (solo lectura)", expanded=True):
            st.dataframe(df_account_cutoffs, use_container_width=True)

            st.markdown("#### Actualizar periodos de corte")
            st.write(
                "Este botón ejecuta `SELECT "
                f"{schema}.refresh_account_cutoffs();` para recalcular "
                "los periodos de corte de todas las cuentas."
            )

            if st.button("Refrescar account_cutoffs", key="btn_refresh_cutoffs"):
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(f"SELECT {schema}.refresh_account_cutoffs();")
                        )
                    st.success("Periodos de corte actualizados correctamente ✅")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al actualizar los periodos de corte: {e}")

    # ==== Debajo: cutoff_days y cutoff_years como antes ====
    # ==== Debajo: cutoff_days y cutoff_years ====

    # --- cutoff_days: editar SOLO cutoff_date de registros existentes ---
    with st.expander("Tabla cutoff_days (modificable)", expanded=False):
        st.write(
            "Puedes ajustar la **fecha de corte** de cada periodo. "
            "El año y el periodo no se pueden modificar."
        )

        editable_cutoff_days = st.data_editor(
            df_cutoff_days,
            use_container_width=True,
            num_rows="fixed",  # no permitir agregar/eliminar filas desde la UI
            column_config={
                "year_value": st.column_config.NumberColumn(disabled=True),
                "period": st.column_config.TextColumn(disabled=True),
            },
        )

        if st.button("Guardar cambios en cutoff_days", key="btn_save_cutoff_days"):
            cambios = []
            for idx, row_original in df_cutoff_days.iterrows():
                row_nueva = editable_cutoff_days.loc[idx]
                if pd.to_datetime(row_original["cutoff_date"]) != pd.to_datetime(row_nueva["cutoff_date"]):
                    cambios.append(
                        (
                            int(row_original["year_value"]),
                            str(row_original["period"]),
                            row_nueva["cutoff_date"],
                        )
                    )

            if not cambios:
                st.info("No hay cambios que guardar.")
            else:
                try:
                    with engine.begin() as conn:
                        for year_value, period, nueva_fecha in cambios:
                            conn.execute(
                                text(f"""
                                    UPDATE {schema}.cutoff_days
                                    SET cutoff_date = :new_date
                                    WHERE year_value = :year_value
                                      AND period = :period;
                                """),
                                {
                                    "new_date": nueva_fecha,
                                    "year_value": year_value,
                                    "period": period,
                                },
                            )
                    st.success("Fechas de corte actualizadas correctamente ✅")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al actualizar cutoff_days: {e}")

    # --- cutoff_years: solo agregar el año actual ---
    with st.expander("Tabla cutoff_years (solo agregar años)", expanded=False):
        st.dataframe(df_cutoff_years, use_container_width=True)

        current_year = date.today().year
        years_present = df_cutoff_years["year_value"].astype(int).tolist()

        if current_year in years_present:
            st.info(f"El año {current_year} ya está registrado en cutoff_years.")
        else:
            if st.button(f"Agregar año actual ({current_year})", key="btn_add_year"):
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(f"""
                                INSERT INTO {schema}.cutoff_years (year_value)
                                VALUES (:year_value)
                                ON CONFLICT (year_value) DO NOTHING;
                            """),
                            {"year_value": current_year},
                        )
                    st.success(f"Año {current_year} agregado correctamente ✅")
                    st.info("El trigger se encargará de generar los días de corte para ese año.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al agregar el año {current_year}: {e}")

# Cerrar conexión
connexion.close()