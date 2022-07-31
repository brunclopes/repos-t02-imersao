# Databricks notebook source
# DBTITLE 1,Importando bibliotecas - Python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de destino 
gold_path_Python = "/mnt/gold/dim_organizador/"

# COMMAND ----------

# DBTITLE 1,Leitura da tabela organizador - Python
organizadorDF = spark.table('silver.organizador').filter('Updated_Date is null')

# COMMAND ----------

# DBTITLE 1,Leitura da tabela telefone - Python
telefoneDF = spark.table('silver.telefone').filter('Updated_Date is null')

# COMMAND ----------

# DBTITLE 1,Exibir schema organizador - Python
organizadorDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Exibir schema telefone- Python
telefoneDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados organizador- Python
organizadorDF.show()
#ou
display(organizadorDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados telefone- Python
telefoneDF.show()
#ou
display(telefoneDF)

# COMMAND ----------

# DBTITLE 1,Join - Python
organizador_joinDF = organizadorDF.join(telefoneDF, organizadorDF.MATRICULA == telefoneDF.MAT_ORGANIZADOR, 'inner')
organizador_joinDF = organizador_joinDF.select('MATRICULA', 'NOME', 'NUMERO')
display(organizador_joinDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos na trusted - Python
organizador_joinDF.write.mode('overwrite').format('parquet').save(gold_path_Python)

# COMMAND ----------

# DBTITLE 1,Consultando arquivos da trusted - Python
dim_organizadorDF = spark.read.format('parquet').load(gold_path_Python)

display(dim_organizadorDF)