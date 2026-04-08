from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from fastapi.responses import HTMLResponse 
from sqlalchemy import create_engine, text
import pandas as pd
import io
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="API Pullman Bus - Reportes")

# 1. Configuración de la base de datos
usuario = os.getenv('DB_USER', 'root')
password = os.getenv('DB_PASSWORD', '')
host = os.getenv('DB_HOST', 'localhost:3306')
base_datos = os.getenv('DB_NAME', 'ventas-pullman')

DB_URL = f"mysql+pymysql://{usuario}:{password}@{host}/{base_datos}"
engine = create_engine(DB_URL)

# Ruta Principal: Muestra el formulario HTML
@app.get("/", response_class=HTMLResponse)
async def main():
    content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Carga de Ventas - Pullman Bus</title>
        <style>
            body { font-family: sans-serif; background-color: #f4f4f4; display: flex; justify-content: center; padding-top: 50px; }
            .card { background: white; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-top: 5px solid orange; }
            h2 { color: #003366; }
            input[type="file"] { margin: 1rem 0; }
            button { background: #003366; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
            button:hover { background: orange; }
        </style>
    </head>
    <body>
        <div class="card">
            <h2>Cargar CSV de Ventas</h2>
            <form action="/upload" enctype="multipart/form-data" method="post">
                <input name="file" type="file" accept=".csv">
                <br>
                <button type="submit">Subir e Importar a MySQL</button>
            </form>
        </div>
    </body>
    </html>
    """
    return content

# Ruta de Procesamiento: Recibe el archivo y lo mete a la BD
@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    try:
        # 1. Leer el archivo directamente desde la memoria
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents), sep=',', dtype=str)

        # 2. Limpieza de nombres de columnas
        df.columns = [c.replace(' ', '_').replace('°', 'N').replace('-', '_').lower() for c in df.columns]

        # 3. Procesar Números (Limpieza integral de símbolos y conversión a INT)
        cols_num = [
            'bus', 'nn_asiento', 'tarifa', 'comisión', 'descuento', 
            'monto_de_impuesto_de_servicio', 'tarifa_anulaciones',
            'el_monto_del_impuesto_del_servicio_cancelado_es'
        ]
        
        for col in cols_num:
            if col in df.columns:
                # Quitamos $ y . para que MySQL acepte el número
                df[col] = df[col].str.replace(r'[\$\.]', '', regex=True).str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # 4. Procesar Fechas (SOLUCIÓN AL ERROR DATETIME)
        # Agregamos todas las columnas que manejan tiempo
        cols_fecha = ['fecha_de_viaje', 'fecha_venta', 'emitido_el', 'fecha_anulación']
        
        for col in cols_fecha:
            if col in df.columns:
                # dayfirst=True es clave para el formato chileno/latino
                # errors='coerce' convierte lo que no entiende en vacío (NULL) en vez de dar error
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

        # 5. Cargar a MySQL
        # Nota: if_exists='append' para ir sumando archivos
        df.to_sql(name='pasajes', con=engine, if_exists='append', index=False, chunksize=10000)

        return {
            "estado": "Exitoso",
            "archivo": file.filename,
            "filas_cargadas": len(df),
            "mensaje": "Los datos han sido integrados correctamente."
        }

    except Exception as e:
        # Esto te mostrará el error exacto en formato JSON si algo falla
        return {"estado": "Error", "detalle": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)