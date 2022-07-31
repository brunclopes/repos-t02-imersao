# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Destino dos arquivos
gold_path_python = "/mnt/gold/dim_calendario/"

# COMMAND ----------

# DBTITLE 1,Cria dataframe com o range de data especificado
calendarioDF = spark.createDataFrame([(1,)], ["id"])

calendarioDF = calendarioDF.withColumn(
    "date", 
    F.explode(F.expr("sequence(to_date('2020-01-01'), to_date('2021-12-31'), interval 1 day)"))
)

# COMMAND ----------

# DBTITLE 1,Consultando dados do dataframe
calendarioDF.show()
#ou
display(calendarioDF)

# COMMAND ----------

# DBTITLE 1,Adiciona colunas formatadas no dataframe
calendarioDF = calendarioDF.drop('id')
calendarioDF = calendarioDF.withColumn('Ano', F.date_format(F.col('date'), 'yyyy'))
calendarioDF = calendarioDF.withColumn('Mes', F.date_format(F.col('date'), 'MM'))
calendarioDF = calendarioDF.withColumn('Dia', F.date_format(F.col('date'), 'dd'))
calendarioDF = calendarioDF.withColumn('Data', F.col('date'))
calendarioDF = calendarioDF.drop('date')

# COMMAND ----------

# DBTITLE 1,Consultando dados do dataframe
calendarioDF.show()
#ou
display(calendarioDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos na trusted - Python
calendarioDF.write.mode('overwrite').format('delta').save(gold_path_python)

# COMMAND ----------

# DBTITLE 1,Consultando arquivos da trusted - Python
dim_calendarioDF = spark.read.format('delta').load(gold_path_python)

display(dim_calendarioDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from gold.dim_calendario