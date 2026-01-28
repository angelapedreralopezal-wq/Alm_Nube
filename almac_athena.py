import boto3
from dotenv import load_dotenv
import os
import time
import json

load_dotenv()

# Conexión a AWS Athena
session = boto3.session.Session(
   aws_access_key_id=os.getenv("ACCESS_KEY"),
   aws_secret_access_key=os.getenv("SECRET_KEY"),
   aws_session_token=os.getenv("SESSION_TOKEN"),
   region_name=os.getenv("REGION"))

athena = session.client('athena')
s3 = session.client('s3')

bucket_name = "presupuestos-bucket"
file_path_csv = "presupuestos.csv"
file_path_json = "presupuestos.jsonl"
s3_key = "datos/presupuestos.csv"
s3_key_json = "datos_json/presupuestos.jsonl"

# Crear bucket S3
def crear_bucket():
   s3.create_bucket(Bucket=bucket_name)
   print("Bucket S3 creado:", bucket_name)
   
   # Subir archivo csv
   # s3.upload_file(file_path_csv, bucket_name, s3_key)

   # Subir archivo json
   s3.upload_file(Bucket=bucket_name, Key="datos_json/")

   print("Archivo subido a S3 correctamente")

# Crear base de datos en Athena
def crear_base_datos():
   query = f"CREATE DATABASE IF NOT EXISTS presupuestos_db"
   athena.start_query_execution(
       QueryString=query,
       ResultConfiguration={'OutputLocation': f's3://{bucket_name}/athena-results/'}
   )
   print("Base de datos creada: presupuestos_db")

# Crear tabla en Athena
def crear_tabla():
   query = f"""
   CREATE EXTERNAL TABLE IF NOT EXISTS presupuestos_db.presupuestos (
      id_presupuesto INT,
      id_departamento INT,
      anio INT,
      monto_asignado DOUBLE,
      monto_utilizado DOUBLE
   )
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
   WITH SERDEPROPERTIES (
      'serialization.format' = ',',
      'field.delim' = ','
   )
   LOCATION 's3://presupuestos-bucket/datos/'
   TBLPROPERTIES ('skip.header.line.count'='1');
   """

   athena.start_query_execution(
      QueryString=query,
      QueryExecutionContext={
         "Database": "presupuestos_db"
      },
      ResultConfiguration={
         "OutputLocation": f"s3://{bucket_name}/athena-results/"
      }
   )
   print("Tabla creada: presupuestos")

# Crear tabla para datos JSON en Athena
# Subir JSON Lines a S3
s3.upload_file(file_path_json, bucket_name, "datos_json/presupuestos.jsonl")
print("Archivo JSON subido correctamente")

# Crear tabla JSON en Athena
def crear_tabla_json():
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS presupuestos_db.presupuestos_jsonl (
        id_presupuesto INT,
        id_departamento INT,
        anio INT,
        monto_asignado DOUBLE,
        monto_utilizado DOUBLE
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES (
        'ignore.malformed.json' = 'true'
    )
    LOCATION 's3://{bucket_name}/datos_json/'
    TBLPROPERTIES ('has_encrypted_data'='false');
    """
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "presupuestos_db"},
        ResultConfiguration={"OutputLocation": f"s3://{bucket_name}/athena-results/"}
    )
    print("Tabla JSON creada correctamente")

# Ejecutar consultas
def ejecutar_consulta(query, database="presupuestos_db"):
   # Iniciar la consulta
   response = athena.start_query_execution(
      QueryString=query,
      QueryExecutionContext={"Database": database},
      ResultConfiguration={
         "OutputLocation": f"s3://{bucket_name}/athena-results/"
      }
   )
    
   query_id = response["QueryExecutionId"]
   print("Query lanzada. ID:", query_id)
    
   # Esperar a que la consulta termine
   estado = 'RUNNING'
   while estado in ['RUNNING', 'QUEUED']:
      time.sleep(2)  # espera 2 segundos
      estado_resp = athena.get_query_execution(QueryExecutionId=query_id)
      estado = estado_resp['QueryExecution']['Status']['State']
    
   if estado == 'SUCCEEDED':
      print("Consulta terminada con éxito.\n")
      # Obtener resultados
      result = athena.get_query_results(QueryExecutionId=query_id)
        
      # Parsear resultados en lista de diccionarios
      columnas = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
      filas = result['ResultSet']['Rows']
        
      datos = []
      for fila in filas[1:]:  # saltar encabezado
         valores = [v.get('VarCharValue', None) for v in fila['Data']]
         datos.append(dict(zip(columnas, valores)))
      
      # Mostrar datos formateados
      for fila in datos:
         print(fila)
      print()
        
      return datos
   else:
      print("Consulta falló:", estado)
      return None

# Convertir JSON a JSON Lines
def convertir_json_a_jsonl():
   # Abrir JSON original
   with open("presupuestos.json", "r") as f:
      data = json.load(f)  # Esto carga un array de dicts

   # Escribir JSON Lines
   with open("presupuestos.jsonl", "w") as f:
      for item in data:
         f.write(json.dumps(item) + "\n")

# crear_bucket()
# crear_base_datos()
# crear_tabla()
convertir_json_a_jsonl()
crear_base_datos()  
crear_tabla_json()

# Ejemplo de consulta
# consulta1 = "SELECT * FROM presupuestos_db.presupuestos LIMIT 10;"
# ejecutar_consulta(consulta1)

# consulta2 = "SELECT anio, SUM(monto_asignado) AS total_asignado, SUM(monto_utilizado) AS total_utilizado FROM presupuestos_db.presupuestos GROUP BY anio;"
# ejecutar_consulta(consulta2)

# consulta3 = "SELECT id_departamento, SUM(monto_asignado) AS total_asignado, SUM(monto_utilizado) AS total_utilizado FROM presupuestos_db.presupuestos GROUP BY id_departamento ORDER BY total_asignado DESC LIMIT 5;"
# ejecutar_consulta(consulta3)