import os
import pandas as pd
from openai import OpenAI
import tiktoken
import streamlit as st
import boto3
from botocore.exceptions import ClientError

OPENAI_API_KEY = st.secrets["openai"]["api_key"]
client = OpenAI(api_key=OPENAI_API_KEY)

prompt_texto_informe = """
ROL:
actúa como un analista senior de inteligencia de negocio de KIA España, especializado en redacción de informes ejecutivos para alta dirección del área de customer experience. tu objetivo es generar un informe claro, estratégico y orientado a decisiones, basado en uno de los archivos de datos proporcionados.

QUE RECIBIRÁ:
    - le vendrá uno de estos documentos y debe utilizar TODA su información:
        - sales_concatenado.xlsx (ventas)
        - service_concatenado.xlsx (servicio postventa)
    - la explicación de algunas de las columnas:
        - Centro: taller o concesionario.
        - Modelo: modelo de coche Kia.
        - Tecnología: eléctrico, híbrido o carburante.
        - Puntuación: valoración del servicio por parte del cliente. de 1 a 8 (1 es muy malo y 8 es muy bueno).
        - Comentario: desarrollo de la opinión sobre el servicio o el coche.
        - Fecha: fecha de la publicación de la valoración.


DEBE HACER:
        - el objetivo es elaborar un informe.
        - el informe debe estar en formato markdown, manteniendo el formato de principio a fin:
               - usar títulos (##), subtítulos (###), listas y bullets.
               - incluir tablas en markdown si aplica.
        - seguir un proceso estructurado de análisis de negocio:
        - tendencias y comparaciones: análisis temporal, entre talleres, modelos o tecnología, según aplique. Cruzar varios campos puede ser interesante.
        - recomendaciones estratégicas: propuestas claras que podrían derivarse del análisis.
        - mantener un tono ejecutivo, directo y profesional, sin tecnicismos ni análisis de estructura de datos.



ESTRUCTURA DEL INFORME:

    - TÍTULO:
	   - con esta estructura: "INFORME DE EXPERIENCIA DE USUARIO DE [LOS MESES Y EL AÑO] EN [CONCESIONARIOS/TALLERES] KIA".
           - después haz una muy breve introducción sobre qué información contiene el archivo, fechas (meses), y por qué es relevante. di que estos datos sale del análisis de los verbatims (opiniones usuarios de Kia) y que pueden estar sesgados voluntariamente por las personas que lo suministran.
           - detectar si es ventas (concesionarios) o es servicio (talleres).

    - HALLAZGOS CLAVE: puntos más importantes y patrones detectados que impactan directamente en el negocio. conociendo el hallazgo busca en los comentarios los motivos y señala con nombres, números y motivos. También puedes cruzar talleres con modelos y ver si hay alguna tendencia.
	    - Identifica cuánto porcentaje del total por taller/modelo/tecnología representan los datos que das.

    - AREAS DE OPORTUNIDAD Y RIESGO: identificar claramente los elementos que requieren atención inmediata:
            - señalar con claridad cualquier taller, modelo o tecnología que tenga puntuaciones muy bajas.
            - nombrar siempre los modelos y talleres con nombre.
	    - Referenciar con datos y porcentajes.
            - si algún taller, coche o modelo tiene puntuaciones críticas (p. ej., promedio por debajo de 5), destacarlo explícitamente e intentar explicar los motivos.
	    - Identifica cuánto porcentaje del total por taller/modelo/tecnología representan los datos que das.

    - TEMPORALIDAD:
            - Busca en los comentarios qué tardanza en el servicio les parece a los clientes que sea mala, cuál esta bien y cuál es genial.
	    - Busca patrones de si se dejaron de hacer las entrevistas en algún momento en algún concesionario/taller y referéncialo con datos.

    - TALLERES/CONCESIONARIOS:
            - Debe decir todos aquellos talleres que tengan una puntuación por debajo de 5 con sus notas ordenándolos de menor a mayor.
            - Señala lo que falla en los talleres, buscando los motivos en los comentarios.
            - Dime también los 3 mejores talleres con sus notas y destaca las cosas que dicen los comentarios que hacen bien.
            - Intenta buscar las diferencias entre los comentarios de los talleres/concesionarios que tienen mala puntuación y los que tienen buena puntuación para ver que es lo más importante.
	    - Identifica cuánto porcentaje del total por taller representan los datos que das.

    - MODELOS Y TECNOLOGÍA: Dedica un apartado a esto, en fallos concretos destacados por modelo y tecnología, si falla en un concesionario/taller concreto y el volumen es relevante, dilo.
	    - Identifica cuánto porcentaje del total por taller representan los datos que das.

    - CONCLUSIONES: una síntesis breve, clara y accionable de todo el informe y cambios que se esperan para próximos informes de manera general y con nombres propios.


NO DEBE HACER:
    - no incluir explicaciones técnicas sobre columnas, tipos de datos o formato del archivo.
    - no usar lenguaje académico, estadístico o de programación.
    - no presentar código ni fórmulas.
    - no extenderse innecesariamente en explicaciones metodológicas.
    - no usar ejemplos genéricos ni ajenos a la industria automotriz.
    - no ignorar o suavizar los puntos negativos. si hay algo crítico, debe señalarse con mucha claridad, buscar todas las razones, dar datos y explicar su impacto.
    - no entres a decir cómo hacer o mejorar su trabajo a concesionarios ni talleres, pero si señala qué partes hay que mejorar del proceso o trabajo con modelo.


CONSIDERACIONES CLAVE:
    - la respuesta entera debe estar en formato MARKDOWN.
    - el informe debe ser útil para la toma de decisiones estratégicas de la dirección general de customer experience de kia.
    - usar lenguaje claro, orientado a negocio: hablar de clientes, ventas, performance, eficiencia, tendencias, márgenes, oportunidades y riesgos.
    - se deben incluir observaciones por regiones, modelos, talleres o meses si los datos lo permiten.
    - el objetivo es resumir e interpretar. la profundidad viene de los insights y los comentarios.
    - cuando haya problemas (como un taller o modelo con puntuaciones muy bajas), se debe explicar el impacto real que puede tener en la experiencia de cliente, la reputación o el negocio y buscar en los comentarios el porqué, intenta que sea un volumen significativo las personas que mencionan ese porqué y referencia con números.
"""


def funcion_informe(df, tipo_servicio):
    # Carpeta donde están los informes
    carpeta_informes = "funciones/informes/informes/"

    # Nos aseguramos de que la carpeta existe (opcional)
    os.makedirs(carpeta_informes, exist_ok=True)

    # Construimos la ruta completa del archivo dentro de la carpeta
    nombre_archivo = f"informe1t{tipo_servicio.lower().replace(' ', '').replace('é', 'e').replace('í', 'i').replace('ó', 'o')}.txt"
    ruta_archivo = os.path.join(carpeta_informes, nombre_archivo)

    # Si ya existe, lo leemos y devolvemos
    if os.path.exists(ruta_archivo):
        with open(ruta_archivo, "r", encoding="utf-8") as f:
            return f.read()
        
    # ---- SI NO EXISTE, GENERAMOS EL INFORME ---- #

    # Convertimos el DataFrame a CSV de texto
    datos_informe = df.to_csv(index=False)

    # Prompt fijo
    prompt = tipo_servicio + prompt_texto_informe + datos_informe

    # Calcular tokens del prompt
    enc = tiktoken.encoding_for_model("gpt-4-turbo")
    prompt_tokens = len(enc.encode(prompt))
    print(f"🧠 Tokens del prompt: {prompt_tokens}")

    # Llamada a OpenAI
    response = client.chat.completions.create(
        model="gpt-4-turbo",  # Asegúrate de usar el modelo adecuado
        messages=[
            {"role": "system", "content": "Eres un analista experto redactando informes ejecutivos para dirección general."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
        max_tokens=500    # Ajusta si tienes acceso a modelos con más tokens
    )

    informe = response.choices[0].message.content


    # Guardamos el informe generado para no repetirlo
    with open(ruta_archivo, "w", encoding="utf-8") as f:
        f.write(informe)

    return informe











# ----------------------------- 1. Cargar y preparar datos -----------------------------

def cargar_datos():
    directorio = 'Docs_brutos/'
    archivos = {
        'KES_Product_Verbatim_TV_202501.xls': 'Enero',
        'KES_Product_Verbatim_TV_202502.xls': 'Febrero',
        'KES_Product_Verbatim_TV_202503.xls': 'Marzo'
    }

    dataframes = []

    for archivo, mes in archivos.items():
        ruta_archivo = os.path.join(directorio, archivo)
        
        if not os.path.exists(ruta_archivo):
            print(f"❌ El archivo '{archivo}' no se encuentra en la carpeta '{directorio}'")
            continue
        
        try:
            df = pd.read_excel(ruta_archivo)
            df['Mes'] = mes
            dataframes.append(df)
            print(f"✅ Archivo '{archivo}' cargado correctamente.")
        except Exception as e:
            print(f"❌ Error al cargar el archivo '{archivo}': {e}")
            continue

    if not dataframes:
        print("❌ No se cargaron archivos. Por favor, verifica los nombres y ubicación de los archivos.")
        return None

    df_total = pd.concat(dataframes, ignore_index=True)
    return df_total

# ----------------------------- 2. Procesar y analizar datos -----------------------------

def analizar_datos(df):
    if df is None or df.empty:
        print("❌ No se puede analizar porque no hay datos cargados.")
        return None
    resumen_por_mes = df.groupby('Mes').size().reset_index(name='Total de registros')
    print("✅ Datos analizados correctamente.")
    return resumen_por_mes

# ----------------------------- 3. Generar gráficos -----------------------------

def generar_grafico(resumen_por_mes):
    if resumen_por_mes is None or resumen_por_mes.empty:
        print("❌ No se puede generar el gráfico porque no hay datos analizados.")
        return

    try:
        plt.figure(figsize=(8,5))
        plt.bar(resumen_por_mes['Mes'], resumen_por_mes['Total de registros'])
        plt.title('Total de Registros por Mes')
        plt.xlabel('Mes')
        plt.ylabel('Total de Registros')
        plt.savefig('Docs_brutos/Informe_Grafico.png')
        plt.close()
        print("✅ Gráfico generado correctamente y guardado en 'Docs_brutos/Informe_Grafico.png'.")
    except Exception as e:
        print(f"❌ Error al generar el gráfico: {e}")

# ----------------------------- 4. Guardar informe -----------------------------

def guardar_informe(df_total, resumen_por_mes):
    try:
        if df_total is not None and not df_total.empty:
            df_total.to_excel('Docs_brutos/Informe_Concatenado.xlsx', index=False)
            print("✅ Informe completo guardado en 'Docs_brutos/Informe_Concatenado.xlsx'.")
        
        if resumen_por_mes is not None and not resumen_por_mes.empty:
            resumen_por_mes.to_excel('Docs_brutos/Resumen_Por_Mes.xlsx', index=False)
            print("✅ Resumen por mes guardado en 'Docs_brutos/Resumen_Por_Mes.xlsx'.")
    except Exception as e:
        print(f"❌ Error al guardar los informes: {e}")

# ----------------------------- 5. Ejecutar el informe manualmente -----------------------------

def generar_informe():
    print("🔄 Iniciando generación de informe...")
    df_total = cargar_datos()
    resumen_por_mes = analizar_datos(df_total)
    generar_grafico(resumen_por_mes)
    guardar_informe(df_total, resumen_por_mes)
    print("✅ Informe generado con éxito.")

# Ejecutar directamente al correr el script
if __name__ == "__main__":
    generar_informe()




def print_informe(tipo_servicio):
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"],
        )

        bucket = "kia-verbatims-data"
        
        if tipo_servicio == "Ventas":
            key = "informes/informeventas.txt"
        elif tipo_servicio == "Servicio técnico":
            key = "informes/informeposventa.txt"
        else:
            st.error("❌ Tipo de servicio no reconocido.")
            return ""

        response = s3.get_object(Bucket=bucket, Key=key)
        contenido = response['Body'].read().decode('utf-8', errors='replace')


        return contenido
    
    except ClientError as e:
        st.error(f"🚫 Error al acceder al archivo S3: {e}")
        return ""

    except Exception as e:
        st.error(f"⚠️ Error inesperado: {e}")
        return ""
