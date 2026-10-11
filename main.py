import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from rapidfuzz.process import extractOne
import csv

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (SparkSession.builder
.appName("Desafio LACEDA")
.master("local[*]")
.config("spark.driver.host", "127.0.0.1")
.config("spark.driver.bindAddress", "127.0.0.1")
.config("spark.pyspark.python", sys.executable)
.config("spark.pyspark.driver.python", sys.executable)
.getOrCreate()
)

dfCNEFE = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("data/base_CNEFE_BA.csv")
dfBusca = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("data/base_busca.csv")

#REMOVENDO COLUNAS QUE NÃO USAREMOS
dfCNEFE = dfCNEFE.drop("NUM_QUADRA","NUM_FACE","COD_UNICO_ENDERECO","COD_UF","COD_DISTRITO","COD_SUBDISTRITO","NOM_COMP_ELEM2","VAL_COMP_ELEM2","NOM_COMP_ELEM3","VAL_COMP_ELEM3","NOM_COMP_ELEM4","VAL_COMP_ELEM4","NOM_COMP_ELEM5","VAL_COMP_ELEM5","LATITUDE","LONGITUDE","NV_GEO_COORD","COD_ESPECIE","DSC_ESTABELECIMENTO","COD_INDICADOR_ESTAB_ENDERECO","COD_INDICADOR_CONST_ENDERECO","COD_INDICADOR_FINALIDADE_CONST","COD_TIPO_ESPECI")

#CRIANDO NOVA COLUNA COM O ENDERECO ORDENADO e COLUNA PARA O SETOR CENSITARIO   
dfBusca = dfBusca.withColumn("enderFormatado", concat_ws(" ", col("consulta_logradouro"), col("consulta_bairro"), col("consulta_numero"), col("consulta_cep")))
dfBusca = dfBusca.withColumn("setor_censitario_encontrado", lit("0"))
dfCNEFE = dfCNEFE.withColumn("enderFormatado", concat_ws(" ", col("NOM_TIPO_SEGLOGR"), col("NOM_TITULO_SEGLOGR"), col("NOM_SEGLOGR"), col("DSC_LOCALIDADE"), col("NUM_ENDERECO"), col("CEP")))


linhasAtualizadas = []

for registro in dfBusca.collect():
    
    #FILTRANDO PARA NÃO FAZER 600MILHOES DE COMPARACOES   
    r = registro.asDict()  
    cep = r["consulta_cep"]
    endereco = r["enderFormatado"]
    
    CNEFEfiltrado = dfCNEFE.filter(col("CEP") == cep).select("enderFormatado", "COD_SETOR").collect()
    listaCandidatos = [(end["enderFormatado"], end["COD_SETOR"]) for end in CNEFEfiltrado]
    
    setorEncontrado = None
    
    if listaCandidatos:
        match = extractOne(endereco, [c[0] for c in listaCandidatos], score_cutoff=60.0)
        if match:
            setorEncontrado = listaCandidatos[match[2]][1]
            
    # ADICIONANDO O SETOR CENSITARIO ENCONTRADO AO REGISTRO
    r["setor_censitario_encontrado"] = setorEncontrado
    linhasAtualizadas.append(r)

#CRIANDO NOVO DATAFRAME COM OS RESULTADOS
caminho_saida = "data/base_busca_final.csv"
colunas = list(linhasAtualizadas[0].keys())

with open(caminho_saida, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=colunas, delimiter=";")
    writer.writeheader()
    writer.writerows(linhasAtualizadas)

print(f"Arquivo gerado com sucesso em: {caminho_saida}")
