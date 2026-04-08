import pandas as pd
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv

load_dotenv()

# 1. Configuracion de Conexion
usuario = os.getenv('DB_USER', 'dbmasteruser')
password = os.getenv('DB_PASSWORD', '!3Ht4}.}P1+8<B4Efy||R7D~?i`wEPs7')
host = os.getenv('DB_HOST', 'ls-d3d93b2afa426ca80dbc79750f6fa955eaf3a3b6.cs9gyyc0moxd.us-east-1.rds.amazonaws.com')
base_datos = os.getenv('DB_NAME', 'ventas-pullman')
tabla = os.getenv('DB_TABLE', 'pasajes')

# Creamos el motor de conexion
engine = create_engine(f'mysql+pymysql://{usuario}:{password}@{host}/{base_datos}')

try:
    # 2. Carga de Archivo
    ruta_csv = 'Ventas 2.csv'
    print(f"--- Iniciando proceso para: {ruta_csv} ---")
    
    start_time = time.time()
    df = pd.read_csv(ruta_csv, sep=',', dtype=str, low_memory=False)
    
    # 3. Normalizacion de Columnas
    df.columns = [c.replace(' ', '_').replace('°', 'N').replace('-', '_').lower() for c in df.columns]
    
    # 4. Procesamiento Numerico (Conversion a INT)
    columnas_numericas = [
        'bus', 'nn_asiento', 'tarifa', 'monto_de_impuesto_de_servicio', 
        'comisión', 'descuento', 'tarifa_anulaciones'
    ]
    
    for col in columnas_numericas:
        if col in df.columns:
            # Limpieza eficiente de simbolos monetarios y puntos
            df[col] = df[col].str.replace(r'[\$\.]', '', regex=True).str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # 5. Procesamiento de Fechas
    columnas_fecha = ['fecha_de_viaje', 'fecha_venta', 'emitido_el']
    for col in columnas_fecha:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    # 6. Resumen de Carga y Escritura en DB
    print(f"Registros detectados: {len(df)}")
    print(f"Escribiendo en tabla '{tabla}'...")
    
    # Usamos chunksize para mejorar la estabilidad en archivos grandes
    df.to_sql(name=tabla, con=engine, if_exists='replace', index=False, chunksize=10000)
    
    end_time = time.time()
    duracion = round(end_time - start_time, 2)
    
    print(f"Proceso finalizado exitosamente.")
    print(f"Tiempo de ejecucion: {duracion} segundos.")

except Exception as e:
    print(f"ERROR DURANTE LA EJECUCION: {str(e)}")