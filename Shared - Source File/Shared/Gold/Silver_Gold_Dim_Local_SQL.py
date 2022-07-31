# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de destino com data atual
 
gold_path_SQL = "/mnt/gold/dim_local/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.gold_path_SQL', gold_path_SQL)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela localização - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_localSQL
# MAGIC AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM silver.localizacao
# MAGIC WHERE Updated_Date is null

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_localSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC   *
# MAGIC FROM tempView_localSQL

# COMMAND ----------

# DBTITLE 1,Removendo dados duplicados - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_local_rmDupSQL
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   COD_LOCAL
# MAGIC   , NOME
# MAGIC   , CIDADE
# MAGIC   , BAIRRO
# MAGIC   , ESTADO
# MAGIC FROM tempView_localSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC   *
# MAGIC from tempView_local_rmDupSQL

# COMMAND ----------

# DBTITLE 1,Inserindo o arquivo na camada gold- SQL
# MAGIC %sql
# MAGIC 
# MAGIC /*
# MAGIC O comando gera o arquivo na camada trusted do data lake, porém, a tabela criada esta no formato delta (delta lake) armazenada no database default
# MAGIC */
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS table_Dim_LocalSQL
# MAGIC LOCATION '/mnt/gold/dim_local/'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM tempView_local_rmDupSQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC   delta.`/mnt/gold/dim_local`

# COMMAND ----------

# DBTITLE 1,Utilizando um comando alternativo pois os acima não realizaram o insert - SQL (Bruno)
# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO delta.`/mnt/gold/dim_local`
# MAGIC SELECT DISTINCT
# MAGIC   COD_LOCAL
# MAGIC   , NOME
# MAGIC   , CIDADE
# MAGIC   , BAIRRO
# MAGIC   , ESTADO
# MAGIC FROM tempView_localSQL