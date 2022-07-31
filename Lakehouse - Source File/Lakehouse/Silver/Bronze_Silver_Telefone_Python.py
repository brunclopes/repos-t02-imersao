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
bronze_path_telefone = "/mnt/bronze/telefone.csv"

#Diretorio de destino
silver_path_parquetFiles = f"/mnt/silver/telefone/{data_formatada}/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_telefone', bronze_path_telefone)
spark.conf.set('var.silver_path_parquetFiles', silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Leitura csv telefone- Python
telefoneDF = spark.read.csv(bronze_path_telefone, header=True, sep=';', inferSchema=True)
telefoneDF.createOrReplaceTempView("tempView_telefonePython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
telefoneDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
telefoneDF.show()
#ou
display(telefoneDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
tempView_telefonePythonDF = spark.sql("select * from tempView_telefonePython")
 
display(tempView_telefonePythonDF)

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas - python
telefoneDF = telefoneDF.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Python
telefoneDF = telefoneDF.withColumn('Insert_Date', date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'))

# COMMAND ----------

# DBTITLE 1,Recriando tempview com as novas colunas - Python
telefoneDF.createOrReplaceTempView("tempView_telefonePython")

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python
display(spark.sql("select * from tempView_telefonePython"))

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(telefoneDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(telefoneDF.filter(telefoneDF.COD_TEL == 2))

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - Python
telefoneDF.withColumn('TAMANHO_NUMERO', length(telefoneDF.NUMERO)).show()
display(telefoneDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos parquet na silver - Python
telefoneDF.write.mode('overwrite').format('parquet').save(silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.read.format('parquet').load(silver_path_parquetFiles))