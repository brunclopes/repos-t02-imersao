# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import date

# COMMAND ----------

today = date.today()

data_formatada = today.strftime("%Y%m%d")

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
silver_path_artista = f"/mnt/silver/artista/{data_formatada}/"
silver_path_estilo_musical = f"/mnt/silver/estilo_musical/{data_formatada}/"

#Diretorio de destino
gold_path_deltaTable = "/mnt/gold/dim_artista/"

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos parquet - artistas - Python
artistaDF = spark.read.format('parquet').load(silver_path_artista)
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Leitura tabela estilos musicais - Python
estilo_musicalDF = spark.read.format('parquet').load(silver_path_estilo_musical)
display(estilo_musicalDF)

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
artistaDF.printSchema()

estilo_musicalDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
artistaDF.show()
#ou
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(artistaDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(artistaDF.filter(artistaDF.COD_ESTILO == 2))

# COMMAND ----------

# DBTITLE 1,Join - Python
artista_joinDF = artistaDF.join(estilo_musicalDF, artistaDF.COD_ESTILO == estilo_musicalDF.COD_ESTILO, how='inner').select(artistaDF.COD_ARTISTA, artistaDF.CPF, artistaDF.NOME, estilo_musicalDF.DESCRICAO, artistaDF.Insert_Date)

display(artista_joinDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos na gold - Python
dim_artistaDF = DeltaTable.forPath(spark, gold_path_deltaTable)

dim_artistaDF.alias('tb')\
    .merge(
        artista_joinDF.alias('df'),
        'tb.COD_ARTISTA = df.COD_ARTISTA'
    )\
    .whenMatchedUpdate(set=
        {
           "tb.Updated_Date": date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss') ,
            "tb.NOME": "df.NOME" ,
            "tb.DESCRICAO": "df.DESCRICAO"
        }
    )\
    .whenNotMatchedInsert(values=
        {
            "COD_ARTISTA": "df.COD_ARTISTA" ,
            "CPF": "df.CPF" ,
            "NOME": "df.NOME" ,
            "DESCRICAO": "df.DESCRICAO" ,
            "Insert_Date": "df.Insert_Date"
        }
     )\
    .execute()

# COMMAND ----------

# DBTITLE 1,Consultando arquivos da gold - python
goldDF = spark.read.format('delta').load(gold_path_deltaTable)

display(goldDF)

# COMMAND ----------

display(spark.table('gold.dim_artista'))