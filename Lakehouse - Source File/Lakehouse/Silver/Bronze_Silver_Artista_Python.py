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
bronze_path_artista = "/mnt/bronze/artista.csv"
 
#Diretorio de destino
silver_path_parquetFiles = f"/mnt/silver/artista/{data_formatada}/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_artista', bronze_path_artista)
spark.conf.set('var.silver_path_parquetFiles', silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Leitura csv artista - Python
artistaDF = spark.read.csv(bronze_path_artista, header=True, sep=';', inferSchema=False)
artistaDF.createOrReplaceTempView("tempView_artistaPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
artistaDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
artistaDF.show()
#ou
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas - python
artistaDF = artistaDF.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Python
artistaDF = artistaDF.withColumn('Insert_Date', date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'))

# COMMAND ----------

# DBTITLE 1,Aplicando tipagem nas colunas de código - python
#Converte as colunas COD_ESTILO e COD_ARISTA paa int
artistaDF = artistaDF.withColumn("COD_ARTISTA",artistaDF.COD_ARTISTA.cast(IntegerType()))
artistaDF = artistaDF.withColumn("COD_ESTILO",artistaDF.COD_ESTILO.cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Recriando tempview com as novas colunas - Python
artistaDF.createOrReplaceTempView("tempView_artistaPython")

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python
display(spark.sql("select * from tempView_artistaPython"))

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(artistaDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(artistaDF.filter(artistaDF.COD_ESTILO == 2))

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - Python
artistaDF.withColumn('TAMANHO_NOME', length(artistaDF.NOME)).show()
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos parquet na silver - Python
artistaDF.write.mode('overwrite').format('parquet').save(silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Consultando arquivos parquet
display(spark.read.format('parquet').load(silver_path_parquetFiles))