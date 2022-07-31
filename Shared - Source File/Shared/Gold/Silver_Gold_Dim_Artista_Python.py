# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de destino
gold_path_deltaTable = "/mnt/gold/dim_artista/"

# COMMAND ----------

# DBTITLE 1,Leitura tabela artistas - Python
artistaDF = spark.table('silver.artista').filter('Updated_Date is null')

display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Leitura tabela estilos musicais - Python
estilo_musicalDF = spark.table('silver.estilo_musical').filter('Updated_Date is null')

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
artista_joinDF = artistaDF.join(estilo_musicalDF, artistaDF.COD_ESTILO == estilo_musicalDF.COD_ESTILO, how='inner').select(artistaDF.COD_ARTISTA, artistaDF.CPF, artistaDF.NOME, estilo_musicalDF.DESCRICAO)

display(artista_joinDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos na gold- Python
artista_joinDF.write.mode('overwrite').format('parquet').save(gold_path_deltaTable)

# COMMAND ----------

# DBTITLE 1,Consultando arquivos da gold - python
goldDF = spark.read.format('parquet').load(gold_path_deltaTable)

display(goldDF)