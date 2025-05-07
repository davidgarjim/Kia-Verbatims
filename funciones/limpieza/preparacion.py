import pandas as pd
import os
from glob import glob

def leer_concatenar_y_preparar() -> tuple[pd.DataFrame, pd.DataFrame]:
    archivos = glob('data/bbdd_bruto/*.xls')  
    lista_sales = []  
    lista_service = []  

    if not archivos:
        print("No se encontraron archivos .xls en la carpeta 'Docs_brutos'.")
        return pd.DataFrame(), pd.DataFrame()  

    for archivo in archivos:
        try:
            hojas = pd.read_excel(archivo, sheet_name=None, engine='xlrd')  
            nombres_hojas = list(hojas.keys())
            
            # Identificar 'Sales' (Hoja 1) y 'Service' (Hoja 2)
            if len(nombres_hojas) >= 1:
                df_sales = hojas[nombres_hojas[0]]
                lista_sales.append(df_sales)
            
            if len(nombres_hojas) >= 2:
                df_service = hojas[nombres_hojas[1]]
                lista_service.append(df_service)
            
            #print(f"Archivo leído exitosamente: {archivo}")
        except Exception as e:
            print(f"Error al leer {archivo}: {e}")

    if not lista_sales and not lista_service:
        print("No se encontraron archivos .xls válidos.")
        return pd.DataFrame(), pd.DataFrame()  

    # Concatenar por separado
    df_sales_concatenado = pd.concat(lista_sales, ignore_index=True) if lista_sales else pd.DataFrame()
    df_service_concatenado = pd.concat(lista_service, ignore_index=True) if lista_service else pd.DataFrame()

    # Modificaciones de fecha
    def fecha(df):    
        df['Date of Response'] = pd.to_datetime(
        df['Date of Response'], 
        format='%d/%m/%Y %H:%M:%S', 
        errors='coerce'
        )
        df['Date'] = df['Date of Response'].dt.date
        df = df.sort_values(by=['Date of Response'], ascending=True)

    fecha(df_sales_concatenado)
    fecha(df_service_concatenado)



    # Columnas a eliminar
    columnas_a_eliminar = ['Customer Name', 'Dealer No.', 'VIN', 'Date of Response']
    df_service_concatenado.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')
    df_sales_concatenado.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')

    # ELiminar índice
    df_sales_concatenado.reset_index(drop=True, inplace=True)
    df_service_concatenado.reset_index(drop=True, inplace=True)

    # Renombrar columnas
    def nombres_columnas(df):
        df.rename(columns={
            'Dealer Name': 'Taller',
            'Model': 'Modelo',
            'Fuel': 'Tecnología',
            'Product Score': 'Puntuación',
            'Verbatim': 'Comentarios',
            'Date': 'Fecha'
        }, inplace=True)
    
    nombres_columnas(df_sales_concatenado)
    nombres_columnas(df_service_concatenado)

    def tecno(df):
        df['Tecnología'] = df['Tecnología'].replace({
        'Petrol': 'Gasolina',
        'Hybrid': 'Híbrido',
        'Electric': 'Eléctrico',
        'Diesel': 'Diésel',
        })
    
    tecno(df_sales_concatenado)
    tecno(df_service_concatenado)

    # Eliminar archivos anteriores si existen
    if os.path.exists('sales_concatenado.xlsx'):
        os.remove('sales_concatenado.xlsx')
    if os.path.exists('docs/service_concatenado.xlsx'):
        os.remove('docs/service_concatenado.xlsx')

    # Guardar los nuevos archivos
    if not df_sales_concatenado.empty:
        df_sales_concatenado.to_excel('data/bbdd_procesado/sales_concatenado.xlsx', index=False)
        
    if not df_service_concatenado.empty:
        df_service_concatenado.to_excel('data/bbdd_procesado/service_concatenado.xlsx', index=False)

    lista_sales.clear()
    lista_service.clear()

    return df_sales_concatenado, df_service_concatenado


if __name__ == "__main__":
    df_sales, df_service = leer_concatenar_y_preparar()
    print("Sales:", df_sales.shape)
    print("Service:", df_service.shape)