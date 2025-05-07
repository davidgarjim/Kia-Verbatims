import os
import numpy as np
import pandas as pd
import faiss
from openai import OpenAI
import streamlit as st

OPENAI_API_KEY = st.secrets["openai"]["api_key"]
client = OpenAI(api_key=OPENAI_API_KEY)

def vectorizar_verbatims(tipo_servicio, archivo_excel):
    carpeta_vectores = "funciones/chatbot/vectores"
    os.makedirs(carpeta_vectores, exist_ok=True)

    nombre_archivo_base = f"vectorizacion1t{tipo_servicio.lower().replace(' ', '').replace('é', 'e').replace('í', 'i').replace('ó', 'o')}"
    archivo_faiss = os.path.join(carpeta_vectores, f"{nombre_archivo_base}.faiss")
    archivo_df = os.path.join(carpeta_vectores, f"{nombre_archivo_base}.pkl")

    # Si ya existen, los cargamos
    if os.path.exists(archivo_faiss) and os.path.exists(archivo_df):
        index = faiss.read_index(archivo_faiss)
        df_embeddings = pd.read_pickle(archivo_df)
        return index, df_embeddings

    # Si no existen, procesamos
    df = pd.read_excel(archivo_excel)

    def construir_texto(row):
        return (
            f"Taller: {row['Taller']} | "
            f"Modelo: {row['Modelo']} | "
            f"Tecnología: {row['Tecnología']} | "
            f"Puntuación: {row['Puntuación']} | "
            f"Comentario: {row['Comentarios']} | "
            f"Fecha: {row['Fecha']}"
        )

    df["texto_enriquecido"] = df.apply(construir_texto, axis=1)

    def obtener_embeddings(textos):
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=textos
        )
        return [np.array(e.embedding, dtype="float32") for e in response.data]

    bloques = [df["texto_enriquecido"].iloc[i:i+100].tolist() for i in range(0, len(df), 100)]
    vectores = []
    for bloque in bloques:
        vectores.extend(obtener_embeddings(bloque))

    vectores_np = np.array(vectores, dtype="float32")
    dimension = vectores_np.shape[1]

    index = faiss.IndexFlatL2(dimension)
    index.add(vectores_np)

    df["embedding"] = vectores

    faiss.write_index(index, archivo_faiss)
    df.to_pickle(archivo_df)

    return index, df

def buscar_verbatims(pregunta, index, df_embeddings, top_k=5):
    def obtener_embeddings(textos):
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=textos
        )
        return [np.array(e.embedding, dtype="float32") for e in response.data]

    emb = obtener_embeddings([pregunta])[0]
    D, I = index.search(np.array([emb]), top_k)
    resultados = df_embeddings.iloc[I[0]]
    return resultados[["Taller", "Modelo", "Tecnología", "Puntuación", "Comentarios", "Fecha"]]

tipo_servicio = "Ventas"
archivo_excel = "sales_concatenado.xlsx"
vectorizar_verbatims(tipo_servicio, archivo_excel)