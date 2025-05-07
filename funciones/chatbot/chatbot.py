import streamlit as st
from openai import OpenAI
from funciones.chatbot.vectorizacion import vectorizar_verbatims, buscar_verbatims
import os

OPENAI_API_KEY = st.secrets["openai"]["api_key"]
client = OpenAI(api_key=OPENAI_API_KEY)

def funcion_chatbot(df_actual, tipo_servicio):
    st.title("GPTKia: Verbatims")

    # Cargar índice y embeddings según el tipo de servicio
    #archivo_excel = "docs/sales_concatenado.xlsx" if tipo_servicio == "Ventas" else "docs/service_concatenado.xlsx"
    #index, df_embeddings = vectorizar_verbatims(tipo_servicio, archivo_excel)

    # Historial de mensajes
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "¡Hola! Pregúntame lo que quieras sobre los verbatims."}]

    # Mostrar mensajes previos
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar="🧠" if message["role"] == "assistant" else "🧑‍💻"):
            st.markdown(message["content"])

    # Capturar nueva pregunta
    if pregunta := st.chat_input("Escribe tu pregunta..."):
        st.session_state.messages.append({"role": "user", "content": pregunta})
        with st.chat_message("user"):
            st.markdown(pregunta)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()

            # Buscar verbatims relevantes
            #resultados = buscar_verbatims(pregunta, index, df_embeddings, top_k=5)

            # Armar texto con contexto
            #verbatims = "\n".join([
            #    f"- ({row['Taller']}, {row['Modelo']}, {row['Tecnología']}, {row['Puntuación']}) {row['Comentarios']}"
            #    for _, row in resultados.iterrows()
            #])

            # Prompt contextual
            prompt_sistema = (
                "Eres un analista senior de inteligencia de negocio especializado en Customer Experience para Kia España. "
                "Responde de forma clara, precisa y orientada a decisiones. "
                "Utiliza exclusivamente los comentarios proporcionados como fuente. "
                "Si no hay suficiente información para responder, indícalo claramente. Mínimo 5 respuestas.\n\n"
                #f"Comentarios relevantes:\n{verbatims}"
            )

            # Recuperar las últimas 2 interacciones previas (si existen)
            historial = st.session_state.messages[-4:] if len(st.session_state.messages) >= 4 else []

            # Armar la conversación para OpenAI
            mensajes = [{"role": "system", "content": prompt_sistema}]
            mensajes += historial
            mensajes.append({"role": "user", "content": pregunta})

            # Llamada a OpenAI
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=mensajes,
                temperature=0.3,
                max_tokens=1024,
            )

            respuesta = response.choices[0].message.content

            # Mostrar y guardar respuesta
            st.session_state.messages.append({"role": "assistant", "content": respuesta})
            st.rerun()  

