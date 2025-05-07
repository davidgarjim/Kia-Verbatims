import polars as pl
import pandas as pd
from pathlib import Path
import pandas as pd
from collections import defaultdict, Counter
from typing import Any
import re
import unidecode
import geoip2.database
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
import json

def preparar_datos2():
    # --- Funciones utilitarias ---
    def normalizar_columnas(df: pl.DataFrame) -> pl.DataFrame:
        columnas_normalizadas = [col.strip().lower().replace(" ", "_") for col in df.columns]
        return df.rename(dict(zip(df.columns, columnas_normalizadas)))

    def forzar_utf8(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns([df[col].cast(pl.Utf8) for col in df.columns])

    def renombrar_columnas(df_pl: pl.DataFrame, diccionario: dict) -> pl.DataFrame:
        columnas_renombradas = {col: diccionario.get(col, col) for col in df_pl.columns}
        return df_pl.rename(columnas_renombradas)

    # --- Rutas y preparación ---
    ruta_base = Path(r"C:\Users\david\Documents\PROYECTOS\KIA\VERBATIMS\data\bbdd_bruto")
    archivos_xls = list(ruta_base.rglob("*.xls"))

    ventas_lista = []
    posventa_lista = []

    # --- Procesamiento principal ---
    for archivo in archivos_xls:
        try:
            hojas = pd.ExcelFile(archivo).sheet_names
            diccionario_preguntas = {}

            if "Questionnaire" in hojas:
                df_q = pd.read_excel(archivo, sheet_name="Questionnaire", usecols=["Display ID", "Questionnaire"])
                diccionario_preguntas = dict(zip(df_q["Display ID"].astype(str), df_q["Questionnaire"].astype(str)))

            if "Ventas" in hojas:
                #print(f"Procesando hoja 'Ventas' en {archivo.name}")
                df_ventas_pd = pd.read_excel(archivo, sheet_name="Ventas")
                df_ventas_pl = pl.DataFrame(df_ventas_pd)
                df_ventas_pl = renombrar_columnas(df_ventas_pl, diccionario_preguntas)
                df_ventas_pl = normalizar_columnas(df_ventas_pl)
                df_ventas_pl = forzar_utf8(df_ventas_pl)
                ventas_lista.append(df_ventas_pl)

            if "Posventa" in hojas:
                #print(f"Procesando hoja 'Posventa' en {archivo.name}")
                df_posventa_pd = pd.read_excel(archivo, sheet_name="Posventa")
                df_posventa_pl = pl.DataFrame(df_posventa_pd)
                df_posventa_pl = renombrar_columnas(df_posventa_pl, diccionario_preguntas)
                df_posventa_pl = normalizar_columnas(df_posventa_pl)
                df_posventa_pl = forzar_utf8(df_posventa_pl)
                posventa_lista.append(df_posventa_pl)

        except Exception as e:
            print(f"Error en archivo {archivo.name}: {e}")

    # --- Unir resultados ---
    df_eventas = pl.concat(ventas_lista, how="diagonal") if ventas_lista else pl.DataFrame()
    df_eposventa = pl.concat(posventa_lista, how="diagonal") if posventa_lista else pl.DataFrame()



    print("Lectura completada. Las bases se encuentran en formato Parquet.")

    def eliminar_filas_similares_condicional(df: pl.DataFrame, umbral_columnas_iguales: int = 30) -> pl.DataFrame:
        claves = ["apellido_cliente", "nombre_cliente"]
        # Si no tenemos ambas columnas, devolvemos df sin tocar
        if not set(claves).issubset(df.columns):
            print(f"⚠️ Omitiendo deduplicación: faltan columnas {claves}")
            return df

        columnas = df.columns
        columnas_comparables = [col for col in columnas if col not in claves]
        filas_filtradas: list[dict[str, Any]] = []

        registros = df.to_dicts()

        grupos = defaultdict(list)
        for fila in registros:
            clave = (fila["apellido_cliente"], fila["nombre_cliente"])
            grupos[clave].append(fila)

        for grupo in grupos.values():
            if len(grupo) <= 1:
                filas_filtradas.extend(grupo)
                continue

            eliminadas = set()
            for i, fila_i in enumerate(grupo):
                if i in eliminadas:
                    continue
                for j in range(i + 1, len(grupo)):
                    if j in eliminadas:
                        continue
                    fila_j = grupo[j]
                    iguales = sum(
                        fila_i.get(col) == fila_j.get(col) and fila_i.get(col) is not None
                        for col in columnas_comparables
                    )
                    if iguales >= umbral_columnas_iguales:
                        eliminadas.add(j)

            for idx, fila in enumerate(grupo):
                if idx not in eliminadas:
                    filas_filtradas.append(fila)

        # Paso crítico: asegurar columnas y tipado coherente
        for fila in filas_filtradas:
            for col in columnas:
                fila.setdefault(col, None)

        # Usar pandas para construir DataFrame homogéneo
        df_homogeneo = pd.DataFrame(filas_filtradas)
        df_homogeneo = df_homogeneo.astype("object")  # evitar que pandas aplique su lógica de inferencia

        # Devolver como polars DataFrame
        return pl.DataFrame(df_homogeneo)

    df_eventas = eliminar_filas_similares_condicional(df_eventas)
    df_eposventa = eliminar_filas_similares_condicional(df_eposventa)

    def convertir_tiempo_a_segundos(df: pl.DataFrame, columna: str = "tiempo_empleado") -> pl.DataFrame:
        """
        Convierte una columna de texto con duración en formato 'Xm Ys' (por ejemplo '2m 15s') a segundos como entero.
        
        - Si solo hay 'Xm', interpreta como minutos.
        - Si solo hay 'Ys', interpreta como segundos.
        - Si ambos faltan, devuelve 0.
        - La columna original se sobrescribe con el valor en segundos (pl.Int64).
        
        Parámetros:
        - df: DataFrame de Polars
        - columna: nombre de la columna a convertir
        
        Retorna:
        - DataFrame con la columna transformada en segundos.
        """
        df = df.with_columns(
            pl.col(columna)
            .cast(pl.Utf8)
            .str.strip_chars()
            .fill_null("")
            .alias(columna)
        )

        df = df.with_columns([
            pl.col(columna).str.extract(r"(\d+)m", 1).cast(pl.Int64).fill_null(0).alias("minutos"),
            pl.col(columna).str.extract(r"(\d+)s", 1).cast(pl.Int64).fill_null(0).alias("segundos")
        ])

        df = df.with_columns(
            (pl.col("minutos") * 60 + pl.col("segundos"))
            .cast(pl.Int64)
            .alias(columna)
        )

        return df.drop(["minutos", "segundos"])

    df_eventas = convertir_tiempo_a_segundos(df_eventas, columna="tiempo_empleado")
    df_eposventa = convertir_tiempo_a_segundos(df_eposventa, columna="tiempo_empleado")

    def limpiar_correos_extendido(df: pl.DataFrame, columna: str = "email") -> pl.DataFrame:
        """
        Limpia la columna de correos eliminando:
        - Strings nulos o vacíos
        - Formatos inválidos
        - Correos con patrones triviales de teclado
        - Dominios falsos o comunes de prueba
        - Correos exactos conocidos como placeholders
        """
        df = df.with_columns(
            pl.col(columna)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .alias(columna)
        )

        # Regex de formato válido simple
        regex_valido = r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"

        # Patrones de teclado
        patrones_malos_inicio = [
            r"^(0123|1234|2345|abcd|asdf)",
            r"^(qwerty|asdfgh|zxcvb)"
        ]
        regex_malos_inicio = "|".join(patrones_malos_inicio)

        # Dominios sospechosos
        dominios_falsos = [
            "@mail.com", "@test.com", "@abc.com", "@qq.com",
            "@correo.com", "@email.com", "@fake.com", "@sinemail.com",
            "@none.com", "@example.com", "@nomail.com", "@noemail.com",
            "@na.com", "@null.com", "@nulo.com", "nocorreo@nocorreo.com",
            "nomail@nomail.com", "noemail@noemail.com", "none@none.com",
            "nocorreo@gmail.com", "nomail@gmail.com", "noemail@gmail.com",
            "none@gmail.com"
        ]
        regex_dominio_falso = "(" + "|".join([re.escape(d) for d in dominios_falsos]) + ")$"

        # Correos falsos exactos
        correos_falsos_exactos = [
            "test@test.com", "correo@correo.com", "email@email.com",
            "sin@email.com", "fake@fake.com", "none@none.com",
            "na@na.com"
        ]

        # Aplicar validaciones
        df = df.with_columns([
            (pl.col(columna).is_not_null() & (pl.col(columna).str.len_chars() > 0)).alias("_no_vacio"),
            pl.col(columna).str.contains(regex_valido).alias("_formato_valido"),
            ~pl.col(columna).str.contains(regex_malos_inicio, literal=False).alias("_sin_patron_inicio"),
            ~pl.col(columna).str.contains(regex_dominio_falso, literal=False).alias("_sin_dominio_falso"),
            ~pl.col(columna).is_in(correos_falsos_exactos).alias("_no_exacto_falso")
        ])

        # Filtrado final
        df_filtrado = df.filter(
            pl.col("_no_vacio") &
            pl.col("_formato_valido") &
            pl.col("_sin_patron_inicio") &
            pl.col("_sin_dominio_falso") &
            pl.col("_no_exacto_falso")
        ).select([col for col in df.columns if not col.startswith("_")])

        return df_filtrado

    df_eventas = limpiar_correos_extendido(df_eventas, columna="email")
    df_eposventa = limpiar_correos_extendido(df_eposventa, columna="email")


    def limpiar_telefonos_es(df: pl.DataFrame, columna: str = "teléfono") -> pl.DataFrame:
        """
        Limpia números de teléfono españoles:
        - Elimina caracteres no numéricos
        - Remueve prefijos internacionales (+34, 0034, 34)
        - Filtra por longitud y formato correcto
        - Elimina patrones sospechosos o genéricos
        """

        # 1. Normalizar números: solo dígitos
        df = df.with_columns(
            pl.col(columna)
            .cast(pl.Utf8)
            .str.replace_all(r"[^\d]", "")  # quitar cualquier cosa que no sea número
            .str.strip_chars()
            .alias(columna)
        )

        # 2. Quitar prefijos internacionales comunes
        df = df.with_columns(
            pl.when(pl.col(columna).str.starts_with("0034"))
            .then(pl.col(columna).str.slice(offset=4))
            .when(pl.col(columna).str.starts_with("34"))
            .then(pl.col(columna).str.slice(offset=2))
            .otherwise(pl.col(columna))
            .alias(columna)
        )

        # 3. Reglas de validación
        regex_valido = r"^\d{9}$"
        patrones_falsos = [
            r"^(12345678|11111111|00000000|98765432|99999999)"
        ]
        regex_patrones_falsos = "|".join(patrones_falsos)

        telefonos_invalidos = [
            "000000000", "123456789", "987654321",
            "666666666", "999999999", "111111111",
            "sintelefono", "ninguno", "noaplica", "no_tengo"
        ]

        # 4. Aplicar filtros
        df = df.with_columns([
            (pl.col(columna).str.len_chars() == 9).alias("_longitud_valida"),
            pl.col(columna).str.contains(regex_valido).alias("_formato_valido"),
            ~pl.col(columna).str.contains(regex_patrones_falsos, literal=False).alias("_no_falso"),
            ~pl.col(columna).is_in(telefonos_invalidos).alias("_no_placeholder"),
            ~(
                (pl.col(columna).str.len_chars() > 9) &
                (
                    pl.col(columna).str.starts_with("6") |
                    pl.col(columna).str.starts_with("7") |
                    pl.col(columna).str.starts_with("9")
                ) &
                pl.col(columna).str.ends_with("0")
            ).alias("_no_sospechoso_extendido")
        ])

        # 5. Filtrar filas válidas
        df_filtrado = df.filter(
            pl.col("_longitud_valida") &
            pl.col("_formato_valido") &
            pl.col("_no_falso") &
            pl.col("_no_placeholder") &
            pl.col("_no_sospechoso_extendido")
        ).select([
            col for col in df.columns if not col.startswith("_")
        ])

        return df_filtrado

    df_eventas = limpiar_telefonos_es(df_eventas, columna="teléfono")
    df_eposventa = limpiar_telefonos_es(df_eposventa, columna="teléfono")

    def reemplazar_guiones_por_null(df: pl.DataFrame) -> pl.DataFrame:
        """
        Reemplaza cualquier celda que contenga solo '-' por null en todo el DataFrame.
        Aplica a todas las columnas del DataFrame, sin importar tipo.
        """
        return df.with_columns([
            pl.when(pl.col(col).cast(pl.Utf8).str.strip_chars() == "-")
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col in df.columns
        ])

    df_eventas = reemplazar_guiones_por_null(df_eventas)
    df_eposventa = reemplazar_guiones_por_null(df_eposventa)

    def normalizar_si_no_bool(df: pl.DataFrame) -> pl.DataFrame:
        """
        Convierte columnas con respuestas tipo Sí/No a booleanos.
        Solo aplica a columnas que contienen valores tipo 's', 'n', 'si', 'y', 'none', ''.
        """
        valores_validos = {"s", "si", "y", "n", "none", ""}
        nuevas_columnas = []

        for col in df.columns:
            # Revisar si la columna contiene principalmente strings tipo sí/no
            muestras = df.select(pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()).unique().to_series()
            muestras_set = set(muestras.drop_nulls().to_list())

            if muestras_set.issubset(valores_validos):
                col_str = pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()

                nueva_col = (
                    pl.when(col_str.is_in(["s", "si", "y"]))
                    .then(True)
                    .when(col_str.is_in(["n", "none", ""]))
                    .then(False)
                    .otherwise(None)
                    .cast(pl.Boolean)
                    .alias(col)
                )

                nuevas_columnas.append(nueva_col)

        return df.with_columns(nuevas_columnas) if nuevas_columnas else df


    df_eventas = normalizar_si_no_bool(df_eventas)
    df_eposventa = normalizar_si_no_bool(df_eposventa)


    # Cargar el archivo de correspondencia modelo → tecnología
    df_tecnologia = pd.read_excel(r"funciones\limpieza\ips\nombres_columnas.xlsx", sheet_name="Tecnología")

    # Convertir a Polars y normalizar nombres
    df_tecnologia_pl = pl.DataFrame(df_tecnologia).rename({
        "Modelo": "modelo",
        "Tecnología": "tecnologia"
    })

    # Asegurarse que la columna "modelo" existe en tus DataFrames
    # Si se llama distinto (ej: 'modelo_coche'), cambialo en el .join
    df_eventas = df_eventas.join(df_tecnologia_pl, on="modelo", how="left")
    df_eposventa = df_eposventa.join(df_tecnologia_pl, on="modelo", how="left")


    def clave_base(s: str) -> str:
        """
        Normaliza una cadena para usarla como clave:
        1) minúsculas
        2) sin acentos
        3) todo no alfanumérico → guión bajo
        4) múltiples guiones bajos colapsados en uno
        """
        t = unidecode.unidecode(s.lower())
        t = re.sub(r'\W+', '_', t)
        return re.sub(r'_+', '_', t).strip('_')


    def clave_base(s: str) -> str:
        """
        Normaliza una cadena para usarla como clave:
        1) minúsculas
        2) sin acentos
        3) todo no alfanumérico → guión bajo
        4) múltiples guiones bajos colapsados en uno
        """
        t = unidecode.unidecode(s.lower())
        t = re.sub(r'\W+', '_', t)
        return re.sub(r'_+', '_', t).strip('_')

    def renombrar_y_fusionar_columnas(
        df: pl.DataFrame,
        mapping_df: pd.DataFrame,
        verbose: bool = False
    ) -> pl.DataFrame:
        """
        1) Calcula para cada mapping_df una 'key' con clave_base(columna_original).
        2) Agrupa en el DataFrame todas las columnas cuya clave_base() coincide.
        3) Para cada grupo:
        - Si hay >1 columna, las fusiona con pl.coalesce(...) bajo el nuevo nombre.
        - Si hay 1 sola, la renombra directamente.
        4) Devuelve df sin errores de columnas duplicadas.
        """
        # 1) Prepara el mapeo normalizado
        m = (
            mapping_df
            .dropna(subset=['columna_original','columna_normalizada'])
            .copy()
        )
        m['key'] = m['columna_original'].astype(str).map(clave_base)
        # key -> nombre final
        key_to_new = dict(zip(m['key'], m['columna_normalizada']))

        # 2) Construye grupos de columnas en df que comparten la misma key
        grupos: dict[str, list[str]] = {}
        for col in df.columns:
            k = clave_base(col)
            if k in key_to_new:
                new_name = key_to_new[k]
                grupos.setdefault(new_name, []).append(col)

        # 3) Fusiona o renombra
        for new_name, cols in grupos.items():
            # deduplicar la lista de cols
            cols = list(dict.fromkeys(cols))
            if len(cols) > 1:
                if verbose:
                    #print(f"Fusionando {cols} → '{new_name}'")
                    df = (
                    df
                    .with_columns(pl.coalesce([pl.col(c) for c in cols]).alias(new_name))
                    .drop(cols)
                    )
            else:
                orig = cols[0]
                if orig != new_name:
                    if verbose:
                        #print(f"Renombrando '{orig}' → '{new_name}'")
                        df = df.rename({orig: new_name})

        return df



    # Cargar las hojas de mapeo
    df_ventas_mapping = pd.read_excel(
        r"funciones\limpieza\ips\nombres_columnas.xlsx",
        sheet_name="Ventas"
    )
    df_posventa_mapping = pd.read_excel(
        r"funciones\limpieza\ips\nombres_columnas.xlsx",
        sheet_name="Posventa"
    )

    # Aplicar mapeo y fusión
    df_eventas = renombrar_y_fusionar_columnas(df_eventas, df_ventas_mapping, verbose=True)
    df_eposventa = renombrar_y_fusionar_columnas(df_eposventa, df_posventa_mapping, verbose=True)
    date_map = {
        "respondido":          (pl.Datetime, "%d/%m/%Y %H:%M:%S"),
        "enviado":             (pl.Date,     "%d/%m/%Y"),
        "recordatorio":        (pl.Date,     "%d/%m/%Y"),
        "fecha_contacto":      (pl.Date,     "%d/%m/%Y"),
        "fecha_de_contacto":   (pl.Date,     "%d/%m/%Y"),
        "fecha_solucionado":   (pl.Date,     "%d/%m/%Y"),
        "fecha_de_solucionado":(pl.Date,     "%d/%m/%Y"),
    }

    # 2) Aplicar parseo con strict=False
    for col, (dtype, fmt) in date_map.items():
        if col in df_eventas.columns:
            df_eventas = df_eventas.with_columns(
                pl.col(col).str.strptime(dtype, fmt, strict=False).alias(col)
            )
        if col in df_eposventa.columns:
            df_eposventa = df_eposventa.with_columns(
                pl.col(col).str.strptime(dtype, fmt, strict=False).alias(col)
            )

    # ——— Después de aplicar el parseo con strict=False ———
    # Ahora separo la columna datetime en fecha y hora
    if "respondido" in df_eventas.columns:
        df_eventas = df_eventas.with_columns([
            # extrae la parte fecha como pl.Date
            pl.col("respondido").dt.date().alias("respondido_fecha"),
            # formatea la parte hora como string "HH:MM:SS"
            pl.col("respondido").dt.strftime("%H:%M:%S").alias("respondido_hora"),
        ])
    if "respondido" in df_eposventa.columns:
        df_eposventa = df_eposventa.with_columns([
            pl.col("respondido").dt.date().alias("respondido_fecha"),
            pl.col("respondido").dt.strftime("%H:%M:%S").alias("respondido_hora"),
        ])


    def geoip(df_eventas: pl.DataFrame, df_eposventa: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        # 1) Rutas locales a las bases MaxMind
        BASE_IPS = Path(__file__).parent / "ips"
        CITY_DB_PATH = BASE_IPS / "GeoLite2-City.mmdb"
        ASN_DB_PATH  = BASE_IPS / "GeoLite2-ASN.mmdb"

        # 2) Instanciar lectores
        reader_city = geoip2.database.Reader(str(CITY_DB_PATH))
        reader_asn  = geoip2.database.Reader(str(ASN_DB_PATH))

        # 3) Lookup de una IP
        def lookup_ip(ip: str) -> dict:
            try:
                c = reader_city.city(ip)
                a = reader_asn.asn(ip)
                return {
                    "ip":               ip,
                    "ip_country_name":  c.country.name,
                    "ip_region":        c.subdivisions.most_specific.name,
                    "ip_city":          c.city.name,
                    "ip_postal_code":   c.postal.code,
                    "ip_latitude":      c.location.latitude,
                    "ip_longitude":     c.location.longitude,
                }
            except Exception:
                return {
                    "ip": ip,
                    **{k: None for k in (
                        "ip_country_iso","ip_country_name","ip_region",
                        "ip_city","ip_postal_code","ip_latitude","ip_longitude",
                        "ip_asn","ip_asn_org"
                    )}
                }

        # 4) Función para enriquecer un Polars DataFrame
        def enrich_ips(df: pl.DataFrame, ip_col: str = "ip", max_workers: int = 8) -> pl.DataFrame:
            ips = (
                df
                .filter(pl.col(ip_col).is_not_null())
                .select(ip_col)
                .unique()
                .to_series()
                .to_list()
            )
            with ThreadPoolExecutor(max_workers=max_workers) as exe:
                info = list(exe.map(lookup_ip, ips))
            geo_df = pl.DataFrame(info)
            return df.join(geo_df, on=ip_col, how="left")

        # 5) Enriquecer y devolver
        df_eventas_enriched   = enrich_ips(df_eventas,   ip_col="ip", max_workers=8)
        df_eposventa_enriched = enrich_ips(df_eposventa, ip_col="ip", max_workers=8)
        return df_eventas_enriched, df_eposventa_enriched

    # justo después de haber hecho el parseo de fechas y separación de hora/fecha:
    df_eventas, df_eposventa = geoip(df_eventas, df_eposventa)

    # 1) Carga tu Excel de rentas:
    renta_pd = pd.read_excel(
        r"src/limpieza/ips/renta.xlsx",
        dtype={"codigo_postal": str}   # asegúrate de que el CP sea string
    )

    # 2) Pásalo a Polars y normaliza nombres:
    renta_pl = pl.DataFrame(renta_pd).rename({
        "codigo_postal":   "ip_postal_code",   # que coincida con la columna de GeoIP
        "renta_media":     "renta_media"       # o como se llame en tu Excel
    })

    # 3) Asegúrate de que ambos sean strings y zero-pad si hace falta:
    renta_pl = renta_pl.with_columns(
        pl.col("ip_postal_code")
        .str.zfill(5)
        .alias("ip_postal_code")
    )

    # 4) Enriquecer ambos DataFrames con la renta:
    df_eventas   = df_eventas.join(renta_pl, on="ip_postal_code", how="left")
    df_eposventa = df_eposventa.join(renta_pl, on="ip_postal_code", how="left")



    # --- Guardar resultados ---
    output_dir = Path(r"C:\Users\david\Documents\PROYECTOS\KIA\VERBATIMS\data\bbdd_procesado")
    output_dir.mkdir(parents=True, exist_ok=True)

    df_eventas.write_parquet(output_dir / "df_eventas.parquet")
    df_eposventa.write_parquet(output_dir / "df_eposventa.parquet")

    return df_eventas, df_eposventa



if __name__ == "__main__":
    # Llamada CORRECTA: sin pasar argumentos
    df_eventas, df_eposventa = preparar_datos()

    # Ahora sí puedes imprimir sus columnas
    print("Columnas en df_eventas:", df_eventas.columns)
    print("Columnas en df_eposventa:", df_eposventa.columns)


