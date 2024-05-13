import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import psutil
import os 

# Inicializa a sessão do Spark
spark = SparkSession.builder.appName("ParseMultiLayoutFile").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

## Monitoramento 
def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 ** 2)  # Convert to MB

def get_cpu_usage():
    process = psutil.Process(os.getpid())
    return process.cpu_percent(interval=1)  # CPU percent over the interval



# Carrega os arquivos
layout_path = "layout.json"
file_path = "file_test_mult_layout.txt"

with open(layout_path, "r") as layout_file:
    layout_data = json.load(layout_file)

# Função para extrair colunas e tipos do layout
def extract_columns_and_types(detail_layout):
    columns = []
    for column_info in detail_layout["COLUNAS"]:
        for column_name, column_props in column_info.items():
            columns.append((column_name, column_props["TYPE"], column_props["SIZE"]))
    return columns

# Função para criar um DataFrame a partir das especificações de colunas
def create_dataframe(lines, detail_type, columns):
    schema = StructType([StructField(column[0], StringType(), True) for column in columns])
    data = []

    for line in lines:
        if line.startswith(detail_type):
            row = []
            for column in columns:
                start, length = column[2]
                row.append(line[start-1:start-1+length].strip())
            data.append(row)
    
    return spark.createDataFrame(data, schema)

# Lê o arquivo de dados
with open(file_path, "r") as file:
    lines = file.readlines()

dataframes = {}
details = layout_data["NOME_DO_ARQUIVO"]["DETALHES"]

# Cria DataFrames para cada detalhe
for detail in details:
    for detail_type, detail_layout in detail.items():
        columns = extract_columns_and_types(detail_layout)
        dataframes[detail_type] = create_dataframe(lines, detail_type, columns)

# Mostra os DataFrames criados
for detail_type, df in dataframes.items():
    print(f"DataFrame for {detail_type}:")
    df.show(truncate=False)
    print(f"Memory Usage: {get_memory_usage()} MB")
    print(f"CPU Usage: {get_cpu_usage()}%")




# Fecha a sessão do Spark
spark.stop()
