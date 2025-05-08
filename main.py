#from funciones.limpieza.preparacion import leer_concatenar_y_preparar
#from funciones.limpieza.preparacion2 import preparar_datos2
from funciones.informes.generar_informe import print_informe
from funciones.chatbot.chatbot import funcion_chatbot
#from funciones.informes.descargar_informe import descargar_informe
from funciones.informes.descargar_informe import descargar_informe_online
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import polars as pl
import boto3


# — Autenticación por contraseña —
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
     st.title("Introduce la contraseña:")
     pwd = st.text_input("Contraseña", type="password")
     
     if st.button("Entrar"):
            if pwd == st.secrets["auth"]["password"]:
                st.session_state.authenticated = True
            else:
                st.error("🔒 Contraseña incorrecta")
     st.stop()

# — Selector de servicio —
st.sidebar.image('media/logo.svg', width=250)
st.sidebar.title('Selecciona el servicio:')
tipo_servicio = st.sidebar.selectbox("Tipo de Servicio", ["Ventas", "Servicio técnico"])
centro = "Concesionario" if tipo_servicio == "Ventas" else "Taller"

# Tus credenciales en secrets.toml
AWS = st.secrets["aws"]
BUCKET = AWS["bucket"]
REGION = AWS["region"]
storage_opts = {
    "key":    AWS["access_key_id"],
    "secret": AWS["secret_access_key"],
    "client_kwargs": {"region_name": REGION}
}
PREFIX = "data/bbdd_procesado"

import boto3
import pyarrow.parquet as pq
from io import BytesIO
import pandas as pd

@st.cache_data
def cargar_parquets_s3() -> tuple[pd.DataFrame, pd.DataFrame]:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
        region_name=st.secrets["aws"]["region"],
    )

    bucket = "kia-verbatims-data"
    prefix = "data/bbdd_procesado"

    def leer_parquet_s3(key: str) -> pd.DataFrame:
        buffer = BytesIO()
        s3.download_fileobj(bucket, f"{prefix}/{key}", buffer)
        buffer.seek(0)
        return pq.read_table(buffer).to_pandas()

    df_ev = leer_parquet_s3("df_eventas.parquet")
    df_posv = leer_parquet_s3("df_eposventa.parquet")

    return df_ev, df_posv


import boto3
import pandas as pd
from io import BytesIO

@st.cache_data
def cargar_excels_s3() -> tuple[pd.DataFrame, pd.DataFrame]:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
        region_name=st.secrets["aws"]["region"],
    )

    bucket = st.secrets["aws"]["bucket"]
    prefix = "data/bbdd_procesado"

    def leer_excel(key: str) -> pd.DataFrame:
        buffer = BytesIO()
        s3.download_fileobj(bucket, f"{prefix}/{key}", buffer)
        buffer.seek(0)
        return pd.read_excel(buffer, engine="openpyxl")

    df_sales   = leer_excel("sales_concatenado.xlsx")
    df_service = leer_excel("service_concatenado.xlsx")

    return df_sales, df_service


# … luego en tu flujo principal:
df_eventas, df_eposventa = cargar_parquets_s3()
df_sales,  df_service = cargar_excels_s3()

# ——— Asignar df_actual y df_actual2 según el servicio ———
if tipo_servicio == 'Ventas':
    df_actual  = df_sales   # o tu df_session_state.df_sales si quieres ambos
    df_actual2 = df_eventas
else:
    df_actual  = df_service
    df_actual2 = df_eposventa

df_sinmodif = df_actual

# Desplegable para seleccionar la segmentación a mostrar
st.sidebar.title('¿Qué segmentación deseas analizar?')
segmentacion = st.sidebar.selectbox('Tipo de Segmentación', ['General', 'Por '+centro , 'GPT - Kia'])

if segmentacion == 'GPT - Kia':
    funcion_chatbot(df_actual, tipo_servicio)


if segmentacion == 'General':
    st.sidebar.title('¿Qué información quieres ver?')
    gen_todo = st.sidebar.checkbox('Resumen e informe', value=True)
    gen_tecnologia = st.sidebar.checkbox('Tecnologías', value=False)
    gen_coche = st.sidebar.checkbox('Modelos de coche', value=False)
    gen_taller = st.sidebar.checkbox(centro, value=False)
    gen_todos_los_comentarios = st.sidebar.checkbox('Ver todos los Comentarios', value=False)

    # ================== GRÁFICOS GENERAL================== #
    if gen_todo:
        #with st.expander("Ver Conclusiones Generales"):
        informe = print_informe(tipo_servicio)
        if informe:
            st.markdown(informe)
    

        #descargar_informe(informe)
        descargar_informe_online(tipo_servicio)

        st.header('Gráficos Generales:')
   
        # =================== COMENTARIOS POR FECHA =================== #
        df_semana = (
                df_actual2
                .filter(pl.col("respondido").is_not_null())
                .with_columns(pl.col("respondido").dt.truncate("1w").alias("Semana"))
                .group_by("Semana")
                .agg(pl.count().alias("Comentarios"))
                .sort("Semana")
            )
        df_semana_pd = df_semana.to_pandas()

        fig1 = px.line(
            df_semana_pd,
            x="Semana",
            y="Comentarios",
            title="Nº de Comentarios por Semana",
            markers=True
        )
        fig1.update_traces(line=dict(color="#C4172C"))
        fig1.update_layout(
            xaxis_title="Semana",
            yaxis_title="Nº de Comentarios",
            yaxis=dict(range=[0, df_semana_pd["Comentarios"].max() + 5])
        )
        st.plotly_chart(fig1, use_container_width=True)


        # =================== SATISFACCIÓN MEDIA POR MODELO =================== #

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_modelo_matriz = (
            df_actual
            .groupby('Modelo')
            .agg(
                Comentarios=('Comentarios', 'count'),
                Puntuacion_Media=('Puntuación', 'mean')
            )
            .reset_index()
            .dropna()
        )

        fig = px.scatter(
            df_modelo_matriz,
            x='Comentarios',
            y='Puntuacion_Media',
            size='Comentarios',
            color='Puntuacion_Media',
            color_continuous_scale=[[0, '#C4172C'], [1, '#57C4AD']],
            hover_name='Modelo',
            title="Nº de Comentarios vs Satisfacción por Modelo"
        )

        fig.update_traces(
            marker=dict(opacity=0.8, line=dict(width=1, color='DarkSlateGrey')),
            textposition=None,
            hoverlabel=dict(bgcolor="white", font_color="black", font_size=13)
        )

        fig.update_layout(
            xaxis_title='Cantidad de Comentarios',
            yaxis_title='Puntuación Media',
            yaxis=dict(range=[1, 8.5]),
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)

        # =================== SATISFACCIÓN MEDIA POR TECNOLOGÍA =================== #

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_tecnologia = (
            df_actual
            .groupby('Tecnología')['Puntuación']
            .mean()
            .reset_index()
            .sort_values(by='Puntuación', ascending=False)
            .round({'Puntuación': 2})
        )

        fig = px.bar(
            df_tecnologia,
            x='Tecnología',
            y='Puntuación',
            title='Satisfacción Promedio por Tecnología',
            text='Puntuación'
        )

        fig.update_traces(marker_color='#C4172C', texttemplate='%{text:.2f}', textposition='outside')

        fig.update_layout(
            yaxis=dict(range=[1, 8.05], title='Puntuación Media'),
            xaxis=dict(title='Tecnología', categoryorder='total descending'),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        st.plotly_chart(fig, use_container_width=True)


        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_taller_matriz = (
            df_actual
            .groupby('Taller')
            .agg(
                Comentarios=('Comentarios', 'count'),
                Puntuacion_Media=('Puntuación', 'mean')
            )
            .reset_index()
            .dropna()
            .round({'Puntuacion_Media': 2})
        )

        fig = px.scatter(
            df_taller_matriz,
            x='Comentarios',
            y='Puntuacion_Media',
            size='Comentarios',
            color='Puntuacion_Media',
            color_continuous_scale=[[0, '#C4172C'], [1, '#57C4AD']],
            hover_name='Taller',
            title="Nº de Comentarios vs Satisfacción por Taller"
        )

        fig.update_traces(
            marker=dict(opacity=0.8, line=dict(width=1, color='DarkSlateGrey')),
            textposition=None,
            hoverlabel=dict(bgcolor="white", font_color="black", font_size=13)
        )

        fig.update_layout(
            xaxis_title='Cantidad de Comentarios',
            yaxis_title='Puntuación Media',
            yaxis=dict(range=[1, 8.5]),
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)


    if gen_tecnologia:
        st.title('Información por Tecnología')
        st.write("")
        df_tecnologia_comentarios = (
            df_actual
            .groupby('Tecnología')
            .size()
            .reset_index(name='Comentarios')
            .sort_values(by='Comentarios', ascending=False)
            .round({'Comentarios': 0})
        )

        fig = px.bar(
            df_tecnologia_comentarios,
            x='Tecnología',
            y='Comentarios',
            title='Nº de Verbatims por Tecnología',
            text='Comentarios'
        )

        fig.update_traces(marker_color='#C4172C', texttemplate='%{text}', textposition='outside')

        fig.update_layout(
            xaxis_title='Tecnología',
            yaxis_title='Cantidad de Verbatims',
            xaxis=dict(categoryorder='total descending'),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        st.plotly_chart(fig, use_container_width=True)

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_tecnologia_media = (
            df_actual
            .groupby('Tecnología')['Puntuación']
            .mean()
            .reset_index()
            .sort_values(by='Puntuación', ascending=False)
            .round({'Puntuación': 2})
        )

        fig = px.bar(
            df_tecnologia_media,
            x='Tecnología',
            y='Puntuación',
            title='Puntuación Media por Tecnología',
            text='Puntuación'
        )

        fig.update_traces(marker_color='#C4172C', texttemplate='%{text:.2f}', textposition='outside')

        fig.update_layout(
            yaxis=dict(range=[0, 10], title='Puntuación Media'),
            xaxis=dict(title='Tecnología', categoryorder='total descending'),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        st.plotly_chart(fig, use_container_width=True)

        st.header("Verbatims por Tecnología")

        tipo_comentario = st.radio("Tipo de comentario", ["Negativos", "Positivos"], index=0)

        tecnologias_disponibles = df_actual['Tecnología'].dropna().unique()
        tecnologia_seleccionada = st.selectbox("Selecciona la tecnología", sorted(tecnologias_disponibles), index=list(sorted(tecnologias_disponibles)).index("Eléctrico") if "Eléctrico" in tecnologias_disponibles else 0)

        if tipo_comentario == "Negativos":
            df_filtrado = df_actual[
                (df_actual['Tecnología'] == tecnologia_seleccionada) &
                (pd.to_numeric(df_actual['Puntuación'], errors='coerce') <= 5) &
                (df_actual['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].copy()

            df_filtrado['Puntuación'] = pd.to_numeric(df_filtrado['Puntuación'], errors='coerce')

            df_filtrado = df_filtrado[
                (df_filtrado['Puntuación'] <= 5) &
                (df_filtrado['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].sort_values(by='Puntuación', ascending=True)

            df_filtrado = df_filtrado.sort_values(by='Puntuación', ascending=True).reset_index(drop=True)

            st.write(f"Comentarios **negativos** para tecnología: **{tecnologia_seleccionada}**")

        else:
            df_filtrado = df_actual[
                (df_actual['Tecnología'] == tecnologia_seleccionada) &
                (pd.to_numeric(df_actual['Puntuación'], errors='coerce') >= 8) &
                (df_actual['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].copy()

            df_filtrado['Puntuación'] = pd.to_numeric(df_filtrado['Puntuación'], errors='coerce')
            df_filtrado = df_filtrado.sort_values(by='Puntuación', ascending=False).reset_index(drop=True)

            st.write(f"Comentarios **positivos** para tecnología: **{tecnologia_seleccionada}**")

        for _, row in df_filtrado.iterrows():
            st.markdown(f"""
            <div style="border: 1px solid #ccc; border-radius: 10px; padding: 15px; margin-bottom: 10px; background-color: #fff5f5;">
                <b> Fecha:</b> {row['Fecha'].date() if pd.notnull(row['Fecha']) else '-'}<br>
                <b> Taller:</b> {row['Taller']}<br>
                <b> Modelo:</b> {row['Modelo']}<br>
                <b> Puntuación:</b> <span style="color:#C4172C; font-weight:bold;">{row['Puntuación']}</span><br><br>
                <b> Comentario:</b><br>
                <div style="margin-top:5px; padding-left:10px;">{row['Comentarios']}</div>
            </div>
            """, unsafe_allow_html=True)

        st.download_button(
            label="📥 Descargar comentarios",
            data=df_filtrado[['Fecha', 'Modelo', 'Taller', 'Puntuación', 'Comentarios']].to_csv(index=False),
            file_name=f'comentarios_{tipo_comentario.lower()}_{tecnologia_seleccionada}.csv',
            mime='text/csv'
        )


    if gen_coche:

        st.title('Información por Modelo')
        st.write("")

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_modelo_matriz = (
            df_actual
            .groupby('Modelo')
            .agg(
                Comentarios=('Comentarios', 'count'),
                Puntuacion_Media=('Puntuación', 'mean')
            )
            .reset_index()
            .dropna()
            .round({'Puntuacion_Media': 2})
        )

        fig = px.scatter(
            df_modelo_matriz,
            x='Comentarios',
            y='Puntuacion_Media',
            size='Comentarios',
            color='Puntuacion_Media',
            color_continuous_scale=[[0, '#C4172C'], [1, '#57C4AD']],
            hover_name='Modelo',
            title="Nº de Comentarios vs Satisfacción por Modelo"
        )

        fig.update_traces(
            marker=dict(opacity=0.8, line=dict(width=1, color='DarkSlateGrey')),
            textposition=None,
            hoverlabel=dict(bgcolor="white", font_color="black", font_size=13)
        )

        fig.update_layout(
            xaxis_title='Cantidad de Comentarios',
            yaxis_title='Puntuación Media',
            yaxis=dict(range=[1, 8.5]),
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)


        df_modelo_comentarios = (
            df_actual
            .groupby('Modelo')
            .size()
            .reset_index(name='Comentarios')
            .sort_values(by='Comentarios', ascending=False)
        )

        fig = px.bar(
            df_modelo_comentarios,
            x='Modelo',
            y='Comentarios',
            title='Nº de Verbatims por Modelo',
            text='Comentarios'
        )

        fig.update_traces(marker_color='#C4172C', texttemplate='%{text}', textposition='outside')

        fig.update_layout(
            xaxis_title='Modelo',
            yaxis_title='Cantidad de Verbatims',
            xaxis=dict(categoryorder='total descending'),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        st.plotly_chart(fig, use_container_width=True)

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_modelo_media = (
            df_actual
            .groupby('Modelo')['Puntuación']
            .mean()
            .reset_index()
            .sort_values(by='Puntuación', ascending=False)
        )

        fig = px.bar(
            df_modelo_media,
            x='Modelo',
            y='Puntuación',
            title='Puntuación Media por Modelo',
            text='Puntuación'
        )

        fig.update_traces(marker_color='#C4172C', texttemplate='%{text:.2f}', textposition='outside')

        fig.update_layout(
            yaxis=dict(range=[0, 10], title='Puntuación Media'),
            xaxis=dict(title='Modelo', categoryorder='total descending'),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )
        st.write("\n\n")
        st.plotly_chart(fig, use_container_width=True)
        st.write("\n\n")

        st.title("Verbatims por Modelo")

        tipo_comentario = st.radio("Tipo de comentario", ["Negativos", "Positivos"], index=0)

        modelos_disponibles = df_actual['Modelo'].dropna().unique()
        modelo_seleccionado = st.selectbox("Selecciona el modelo", sorted(modelos_disponibles), index=0)

        if tipo_comentario == "Negativos":
            df_filtrado = df_actual[
                (df_actual['Modelo'] == modelo_seleccionado) &
                (pd.to_numeric(df_actual['Puntuación'], errors='coerce') <= 5) &
                (df_actual['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].copy()

            df_filtrado['Puntuación'] = pd.to_numeric(df_filtrado['Puntuación'], errors='coerce')
            df_filtrado = df_filtrado.sort_values(by='Puntuación', ascending=True).reset_index(drop=True)

            st.write(f"\nComentarios **negativos** para modelo: **{modelo_seleccionado}**")

        else:
            df_filtrado = df_actual[
                (df_actual['Modelo'] == modelo_seleccionado) &
                (pd.to_numeric(df_actual['Puntuación'], errors='coerce') >= 8) &
                (df_actual['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].copy()

            df_filtrado['Puntuación'] = pd.to_numeric(df_filtrado['Puntuación'], errors='coerce')
            df_filtrado = df_filtrado.sort_values(by='Puntuación', ascending=False).reset_index(drop=True)

            st.write(f"Comentarios **positivos** para modelo: **{modelo_seleccionado}**")

        for _, row in df_filtrado.iterrows():
            st.markdown(f"""
            <div style="border: 1px solid #ccc; border-radius: 10px; padding: 15px; margin-bottom: 10px; background-color: #fff5f5;">
                <b> Fecha:</b> {row['Fecha'].date() if pd.notnull(row['Fecha']) else '-'}<br>
                <b> Taller:</b> {row['Taller']}<br>
                <b> Tecnología:</b> {row['Tecnología']}<br>
                <b> Puntuación:</b> <span style="color:#C4172C; font-weight:bold;">{row['Puntuación']}</span><br><br>
                <b> Comentario:</b><br>
                <div style="margin-top:5px; padding-left:10px;">{row['Comentarios']}</div>
            </div>
            """, unsafe_allow_html=True)
            
        st.write("\n\n")
        st.download_button(
            label="Descargar comentarios",
            data=df_filtrado[['Fecha', 'Modelo', 'Taller', 'Tecnología', 'Puntuación', 'Comentarios']].to_csv(index=False),
            file_name=f'comentarios_{tipo_comentario.lower()}_{modelo_seleccionado}.csv',
            mime='text/csv'
        )

    if gen_taller:
        st.title('Información por ' + centro)
        st.write("")

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        df_taller_matriz = (
            df_actual
            .groupby('Taller')
            .agg(
                Comentarios=('Comentarios', 'count'),
                Puntuacion_Media=('Puntuación', 'mean')
            )
            .reset_index()
            .dropna()
            .round({'Puntuacion_Media': 2})
        )

        fig = px.scatter(
            df_taller_matriz,
            x='Comentarios',
            y='Puntuacion_Media',
            size='Comentarios',
            color='Puntuacion_Media',
            color_continuous_scale=[[0, '#C4172C'], [1, '#57C4AD']],
            hover_name='Taller',
            title="Nº de Comentarios vs Satisfacción por Taller"
        )

        fig.update_traces(
            marker=dict(opacity=0.8, line=dict(width=1, color='DarkSlateGrey')),
            textposition=None,
            hoverlabel=dict(bgcolor="white", font_color="black", font_size=13)
        )

        fig.update_layout(
            xaxis_title='Cantidad de Comentarios',
            yaxis_title='Puntuación Media',
            yaxis=dict(range=[1, 8.5]),
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)
        st.header("Verbatims por " + centro)
        st.write("")

        tipo_comentario = st.radio("Tipo de comentario", ["Todos", "Negativos", "Positivos"], index=1)

        talleres_disponibles = sorted(df_actual['Taller'].dropna().unique())
        talleres_disponibles.insert(0, "TODOS")
        taller_seleccionado = st.selectbox("Selecciona el taller", talleres_disponibles)

        df_filtrado = df_actual.copy()

        # Filtrar por taller (si no es "Todos")
        if taller_seleccionado != "Todos":
            df_filtrado = df_filtrado[df_filtrado['Taller'] == taller_seleccionado]

        # Filtrar por tipo de comentario
        df_filtrado['Puntuación'] = pd.to_numeric(df_filtrado['Puntuación'], errors='coerce')

        if tipo_comentario == "Negativos":
            df_filtrado = df_filtrado[
                (df_filtrado['Puntuación'] <= 5) &
                (df_filtrado['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].sort_values(by='Puntuación', ascending=True)

            st.write(f"\nComentarios **negativos** para {'todos los talleres' if taller_seleccionado == 'Todos' else 'el taller: ' + taller_seleccionado}")

        elif tipo_comentario == "Positivos":
            df_filtrado = df_filtrado[
                (df_filtrado['Puntuación'] >= 8) &
                (df_filtrado['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3))
            ].sort_values(by='Puntuación', ascending=False)

            st.write(f"\nComentarios **positivos** para {'todos los talleres' if taller_seleccionado == 'Todos' else 'el taller: ' + taller_seleccionado}")

        else:  # Todos los comentarios
            df_filtrado = df_filtrado[
                df_filtrado['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3)
            ].sort_values(by='Puntuación', ascending=True)

            st.write(f"\nTodos los comentarios para {'todos los talleres' if taller_seleccionado == 'Todos' else 'el taller: ' + taller_seleccionado}")

        # Mostrar comentarios
        for _, row in df_filtrado.iterrows():
            st.markdown(f"""
            <div style="border: 1px solid #ccc; border-radius: 10px; padding: 15px; margin-bottom: 10px; background-color: #fff5f5;">
                <b> Fecha:</b> {row['Fecha'].date() if pd.notnull(row['Fecha']) else '-'}<br>
                <b> Taller:</b> {row['Taller']}<br>
                <b> Modelo:</b> {row['Modelo']}<br>
                <b> Tecnología:</b> {row['Tecnología']}<br>
                <b> Puntuación:</b> <span style="color:#C4172C; font-weight:bold;">{row['Puntuación']}</span><br><br>
                <b> Comentario:</b><br>
                <div style="margin-top:5px; padding-left:10px;">{row['Comentarios']}</div>
            </div>
            """, unsafe_allow_html=True)

        st.download_button(
            label="📥 Descargar comentarios",
            data=df_filtrado[['Fecha', 'Modelo', 'Taller', 'Tecnología', 'Puntuación', 'Comentarios']].to_csv(index=False),
            file_name=f'comentarios_{tipo_comentario.lower()}_{taller_seleccionado.replace(" ", "_").lower()}.csv',
            mime='text/csv'
        )


    if gen_todos_los_comentarios:
        st.title('Comentarios por '+ centro + ':')
        st.dataframe(df_actual[['Fecha', 'Taller', 'Puntuación', 'Comentarios']].drop_duplicates().reset_index(drop=True))
        st.download_button('Descargar Comentarios', df_actual[['Fecha', 'Taller', 'Puntuación', 'Comentarios']].drop_duplicates().reset_index(drop=True).to_csv(index=False), file_name='comentarios_taller.csv', mime='text/csv')


elif segmentacion == 'Por ' + centro:

    st.sidebar.title('Selecciona el taller')
    
    talleres_disponibles = sorted(df_actual['Taller'].dropna().unique())
    talleres_disponibles.insert(0, "TODOS")  # Agrega "Todos" al inicio

    taller = st.sidebar.selectbox('Selecciona el taller:', talleres_disponibles)

    if taller != "TODOS":
        df_actual = df_actual[df_actual['Taller'] == taller]


    st.sidebar.title('¿Qué información quieres ver?')
    todo = st.sidebar.checkbox('Resumen', value=True)
    #tecnologia = st.sidebar.checkbox('Por Tecnología', value=False)
    #coche = st.sidebar.checkbox('Por Coche', value=False)
    todos_los_comentarios_taller = st.sidebar.checkbox('Todos los Comentarios', value=False)

    # ================== GRÁFICOS ================== #
    if todo:
        st.title(taller)
        st.write("")

        df_actual['Fecha'] = pd.to_datetime(df_actual['Fecha'], errors='coerce')
        df_sinmodif['Fecha'] = pd.to_datetime(df_sinmodif['Fecha'], errors='coerce')

        # Rango completo de semanas
        fecha_inicio = df_sinmodif['Fecha'].min().to_period('W').start_time
        fecha_fin = df_sinmodif['Fecha'].max().to_period('W').start_time
        semanas_completas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='W-MON')  # lunes de cada semana

        # Agrupación real por semana
        df_semanal = (
            df_actual
            .groupby(df_actual['Fecha'].dt.to_period('W').apply(lambda r: r.start_time))
            .size()
            .reindex(semanas_completas, fill_value=0)
            .reset_index()
            .rename(columns={'index': 'Semana', 0: 'Comentarios'})
        )

        # Gráfico
        fig = px.line(
            df_semanal,
            x='Semana',
            y='Comentarios',
            title='Nº de Comentarios por Semana',
            markers=True
        )
        fig.update_traces(line=dict(color='#C4172C'))

        fig.update_layout(
            xaxis_title='Semana',
            yaxis_title='Nº de Comentarios',
            yaxis=dict(range=[0, max(df_semanal['Comentarios']) + 2]),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        st.plotly_chart(fig, use_container_width=True)

                # ---------- Comparativa Puntuación por Tecnología: Taller vs Resto ----------

        df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')

        tecnologias_ordenadas = ['Gasolina', 'Diésel', 'Híbrido', 'Eléctrico']
        df_comp = []

        if taller != "TODOS":
            for tecnologia in tecnologias_ordenadas:
                df_tec = df_sinmodif[df_sinmodif['Tecnología'] == tecnologia]

                media_taller = df_tec[df_tec['Taller'] == taller]['Puntuación'].mean()
                media_resto = df_tec['Puntuación'].mean()

                df_comp.append({
                    'Tecnología': tecnologia,
                    'Puntuacion_Taller': round(media_taller, 2) if pd.notna(media_taller) else 0,
                    'Puntuacion_Resto': round(media_resto, 2) if pd.notna(media_resto) else 0
                })



            df_comp = pd.DataFrame(df_comp)

            fig_comp = go.Figure()

            fig_comp.add_trace(go.Bar(
                x=df_comp['Tecnología'],
                y=df_comp['Puntuacion_Taller'],
                name=f'{taller}',
                marker_color='#C4172C',
                text=df_comp['Puntuacion_Taller'],
                textposition='outside'
            ))

            fig_comp.add_trace(go.Bar(
                x=df_comp['Tecnología'],
                y=df_comp['Puntuacion_Resto'],
                name='Media Resto Talleres',
                marker_color='lightgrey',
                text=df_comp['Puntuacion_Resto'],
                textposition='outside'
            ))

            fig_comp.update_layout(
                barmode='group',
                xaxis_title='Tecnología',
                yaxis_title='Puntuación Media',
                yaxis=dict(range=[1, 8.1]),
                title=f'Satisfacción por Tecnología en {taller} vs Resto',
                uniformtext_minsize=8,
                uniformtext_mode='hide',
                showlegend=True
            )

            st.plotly_chart(fig_comp, use_container_width=True)

            # ---------- Comparativa Puntuación por Modelo: Taller vs Resto ----------

            df_actual['Puntuación'] = pd.to_numeric(df_actual['Puntuación'], errors='coerce')
            df_sinmodif['Puntuación'] = pd.to_numeric(df_sinmodif['Puntuación'], errors='coerce')

            df_comp_modelos = []

            if taller != "TODOS":
                # Solo modelos que tienen registros en este taller
                modelos_filtrados = (
                    df_sinmodif[df_sinmodif['Taller'] == taller]['Modelo']
                    .dropna()
                    .unique()
                )

                for modelo in sorted(modelos_filtrados):
                    df_modelo_total = df_sinmodif[df_sinmodif['Modelo'] == modelo]

                    media_taller = df_modelo_total[df_modelo_total['Taller'] == taller]['Puntuación'].mean()
                    media_resto = df_modelo_total[df_modelo_total['Taller'] != taller]['Puntuación'].mean()

                    df_comp_modelos.append({
                        'Modelo': modelo,
                        'Puntuacion_Taller': round(media_taller, 2) if pd.notna(media_taller) else 0,
                        'Puntuacion_Resto': round(media_resto, 2) if pd.notna(media_resto) else 0
                    })

                df_comp_modelos = pd.DataFrame(df_comp_modelos)
                df_comp_modelos = df_comp_modelos.sort_values(by='Puntuacion_Taller', ascending=True)

                fig_modelo = go.Figure()

                fig_modelo.add_trace(go.Bar(
                    x=df_comp_modelos['Modelo'],
                    y=df_comp_modelos['Puntuacion_Taller'],
                    name=f'{taller}',
                    marker_color='#C4172C',
                    text=df_comp_modelos['Puntuacion_Taller'],
                    textposition='outside'
                ))

                fig_modelo.add_trace(go.Bar(
                    x=df_comp_modelos['Modelo'],
                    y=df_comp_modelos['Puntuacion_Resto'],
                    name='Media Resto Talleres',
                    marker_color='lightgrey',
                    text=df_comp_modelos['Puntuacion_Resto'],
                    textposition='outside'
                ))

                fig_modelo.update_layout(
                    barmode='group',
                    xaxis_title='Modelo',
                    yaxis_title='Puntuación Media',
                    yaxis=dict(range=[1, 8.1]),                    
                    title=f'Satisfacción por Modelo en {taller} vs Resto',
                    uniformtext_minsize=8,
                    uniformtext_mode='hide',
                    showlegend=True
                )

                st.plotly_chart(fig_modelo, use_container_width=True)

        # ---------- Matriz por Usuario (cada punto un comentario) ----------

        df_individual = df_actual[
            df_actual['Comentarios'].astype(str).apply(lambda x: len(x.split()) > 3)
        ].copy()

        df_individual['Puntuación'] = pd.to_numeric(df_individual['Puntuación'], errors='coerce')

        fig_usuarios = px.scatter(
            df_individual,
            x='Puntuación',
            y='Modelo',
            color='Puntuación',
            hover_data=['Comentarios'],
            color_continuous_scale=[[0, '#C4172C'], [1, '#57C4AD']],
            title='Distribución Individual de Comentarios'
        )
        fig_usuarios.update_traces(marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')))
        fig_usuarios.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_usuarios, use_container_width=True)



    if todos_los_comentarios_taller:
        st.title('Comentarios por Taller')
        st.dataframe(df_actual[['Fecha', 'Puntuación', 'Comentarios']].drop_duplicates().reset_index(drop=True))
        st.download_button('Descargar Comentarios', df_actual[['Fecha', 'Puntuación', 'Comentarios']].drop_duplicates().reset_index(drop=True).to_csv(index=False), file_name='comentarios_taller.csv', mime='text/csv')