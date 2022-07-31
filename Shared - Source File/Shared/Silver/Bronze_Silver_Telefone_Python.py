# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
bronze_path_telefone = "/mnt/bronze/telefone.csv"

#Diretorio de destino
silver_path_deltaTable = "/mnt/silver/db/telefone/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_telefone', bronze_path_telefone)
spark.conf.set('var.silver_path_deltaTable', silver_path_deltaTable)

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
telefoneDF = telefoneDF.withColumn('Updated_Date', lit(None).cast(StringType()))
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

# DBTITLE 1,Adicionando campo de controle - Python
telefoneDF = telefoneDF.withColumn('Partition_Date', date_format(current_timestamp(), 'yyyyMM'))
telefoneDF = telefoneDF.withColumn('Partition_Date', telefoneDF.Partition_Date.cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Atualizando coluna da tabela - Updated_Date - python
delta_telefoneDF = DeltaTable.forPath(spark, silver_path_deltaTable)

delta_telefoneDF.alias('tb')\
    .merge(
        telefoneDF.alias('df'),
        'tb.COD_TEL = df.COD_TEL'
    )\
    .whenMatchedUpdate(set=
        {
           "tb.Updated_Date": date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')
        }
    )\
    .execute()

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - python
telefoneDF.write.mode('append').format('delta').partitionBy('Partition_Date').save(silver_path_deltaTable)

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.read.format('delta').load(silver_path_deltaTable))

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.table('silver.telefone'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC truncate table silver.telefone