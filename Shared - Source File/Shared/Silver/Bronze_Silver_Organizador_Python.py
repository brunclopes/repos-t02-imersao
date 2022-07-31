# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
bronze_path_organizador = "/mnt/bronze/organizador.csv"

#Diretorio de destino
silver_path_deltaTable = "/mnt/silver/db/organizador/"

# COMMAND ----------

# DBTITLE 1,Leitura csv estilo_musical - Python
organizadorDF = spark.read.csv(bronze_path_organizador, header=True, sep=';', inferSchema=True)
organizadorDF.createOrReplaceTempView("tempView_organizadorPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
organizadorDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
organizadorDF.show()
#ou
display(organizadorDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
tempView_organizadorPythonDF = spark.sql("select * from tempView_organizadorPython")
 
display(tempView_organizadorPythonDF)

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas - python
organizadorDF = organizadorDF.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Python
organizadorDF = organizadorDF.withColumn('Updated_Date', lit(None).cast(StringType()))
organizadorDF = organizadorDF.withColumn('Insert_Date', date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'))

# COMMAND ----------

# DBTITLE 1,Recriando tempview com as novas colunas - Python
organizadorDF.createOrReplaceTempView("tempView_organizadorPython")

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python
display(spark.sql("select * from tempView_organizadorPython"))

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(organizadorDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(organizadorDF.filter(organizadorDF.MATRICULA == 2))

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - Python
organizadorDF.withColumn('TAMANHO_NOME', length(organizadorDF.NOME)).show()
display(organizadorDF)

# COMMAND ----------

# DBTITLE 1,Adicionando campo de controle - Python
organizadorDF = organizadorDF.withColumn('Partition_Date', date_format(current_timestamp(), 'yyyyMM'))
organizadorDF = organizadorDF.withColumn('Partition_Date', organizadorDF.Partition_Date.cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Atualizando coluna da tabela - Updated_Date - python
delta_organizadorDF = DeltaTable.forPath(spark, silver_path_deltaTable)

delta_organizadorDF.alias('tb')\
    .merge(
        organizadorDF.alias('df'),
        'tb.MATRICULA = df.MATRICULA'
    )\
    .whenMatchedUpdate(set=
        {
           "tb.Updated_Date": date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')
        }
    )\
    .execute()

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - python
organizadorDF.write.mode('append').format('delta').partitionBy('Partition_Date').save(silver_path_deltaTable)

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.read.format('delta').load(silver_path_deltaTable))

# COMMAND ----------

# DBTITLE 1,Consultando dados dos arquivos no datalake - python
display(spark.table('silver.organizador').orderBy('MATRICULA'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC truncate table silver.organizador