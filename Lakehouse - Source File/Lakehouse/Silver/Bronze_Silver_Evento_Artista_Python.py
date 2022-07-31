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
bronze_path_evento_artista = "/mnt/bronze/evento_artista.csv"
 
#Diretorio de destino
silver_path_parquetFiles = f"/mnt/silver/evento_artista/{data_formatada}/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_evento_artista', bronze_path_evento_artista)
spark.conf.set('var.silver_path_parquetFiles', silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Leitura csv estilo_musical - Python
evento_artistaDF = spark.read.csv(bronze_path_evento_artista, header=True, sep=';', inferSchema=True)
evento_artistaDF.createOrReplaceTempView("tempView_evento_artistaPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
evento_artistaDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
evento_artistaDF.show()
#ou
display(evento_artistaDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
tempView_evento_artistaPythonDF = spark.sql("select * from tempView_evento_artistaPython")
 
display(tempView_evento_artistaPythonDF)

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas - python
evento_artistaDF = evento_artistaDF.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Convertendo colunas - python
evento_artistaDF = evento_artistaDF.withColumn('DT_PGTO', evento_artistaDF.DT_PGTO.cast(TimestampType()))

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Python
evento_artistaDF = evento_artistaDF.withColumn('Insert_Date', date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'))

# COMMAND ----------

# DBTITLE 1,Recriando tempview com as novas colunas - Python
evento_artistaDF.createOrReplaceTempView("tempView_evento_artistaPython")

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python
display(spark.sql("select * from tempView_evento_artistaPython"))

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(evento_artistaDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(evento_artistaDF.filter(evento_artistaDF.CODIGO == 2))

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos parquet na silver - Python
evento_artistaDF.write.mode('overwrite').format('parquet').save(silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.read.format('parquet').load(silver_path_parquetFiles))