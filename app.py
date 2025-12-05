import streamlit as st

st.set_page_config(
    page_title="Asistente de datos bancarios",
    page_icon="🏦",
)

st.title("Asistente de datos bancarios")
st.write(f"🔎 Streamlit version: {st.__version__}")

page_purpose = """
- Mantén el control de dus datos bancarios (pdf's de estados de cuenta y csv's de transacciones).
- La app se encarga de ayudarte a mantenerlos actualizados
- Tú enfócate en que tomar decisiones financieras basadas en tus datos.
"""
st.markdown(page_purpose)

st.divider()

st.subheader("Navegación")

st.page_link(
    "pages/00_database.py",
    label="Bases de datos",
    icon="💾",
)

st.page_link(
    "pages/01_initialization.py",
    label="Inicia aquí con lo básico",
    icon="📦",
)

