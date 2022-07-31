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
bronze_path_estilo_musical = "/mnt/bronze/estilo_musical.csv"
 
#Diretorio de destino
silver_path_parquetFiles = f"/mnt/silver/estilo_musical/{data_formatada}/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_estilo_musical', bronze_path_estilo_musical)
spark.conf.set('var.silver_path_parquetFiles', silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Leitura csv estilo_musical - Python
estilo_musicalDF = spark.read.csv(bronze_path_estilo_musical, header=True, sep=';', inferSchema=True)
estilo_musicalDF.createOrReplaceTempView("tempView_estilo_musicalPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
estilo_musicalDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
estilo_musicalDF.show()
#ou
display(estilo_musicalDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
tempView_estilo_musicalPythonDF = spark.sql("select * from tempView_estilo_musicalPython")
 
display(tempView_estilo_musicalPythonDF)

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas - python
estilo_musicalDF = estilo_musicalDF.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Python
estilo_musicalDF = estilo_musicalDF.withColumn('Insert_Date', date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'))

# COMMAND ----------

# DBTITLE 1,Recriando tempview com as novas colunas - Python
estilo_musicalDF.createOrReplaceTempView("tempView_estilo_musicalPython")

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python
display(spark.sql("select * from tempView_estilo_musicalPython"))

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(estilo_musicalDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(estilo_musicalDF.filter(estilo_musicalDF.COD_ESTILO == 2))

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - Python
estilo_musicalDF.withColumn('TAMANHO_DESCRICAO', length(estilo_musicalDF.DESCRICAO)).show()
display(estilo_musicalDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos parquet na silver - Python
estilo_musicalDF.write.mode('overwrite').format('parquet').save(silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.read.format('parquet').load(silver_path_parquetFiles))