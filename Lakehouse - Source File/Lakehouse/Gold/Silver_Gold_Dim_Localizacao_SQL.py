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
silver_path_local = f"/mnt/silver/localizacao/{data_formatada}/"

#Diretorio de destino
gold_path_SQL = "/mnt/gold/dim_local/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.gold_path_SQL', gold_path_SQL)
spark.conf.set('var.silver_path_local', silver_path_local)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela localização - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_localSQL USING parquet
# MAGIC OPTIONS (path '${var.silver_path_local}')

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
# MAGIC   , Insert_Date
# MAGIC FROM tempView_localSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC   *
# MAGIC from tempView_local_rmDupSQL

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO gold.dim_local as tb
# MAGIC USING tempView_local_rmDupSQL as dl
# MAGIC   ON tb.COD_LOCAL = dl.COD_LOCAL
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   tb.NOME = dl.NOME ,
# MAGIC   tb.CIDADE = dl.CIDADE ,
# MAGIC   tb.BAIRRO = dl.BAIRRO ,
# MAGIC   tb.ESTADO = dl.ESTADO ,
# MAGIC   tb.Updated_Date = date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC   (
# MAGIC     COD_LOCAL ,
# MAGIC     NOME ,
# MAGIC     CIDADE ,
# MAGIC     BAIRRO ,
# MAGIC     ESTADO ,
# MAGIC     Insert_Date
# MAGIC   )
# MAGIC   VALUES
# MAGIC   (
# MAGIC     dl.COD_LOCAL ,
# MAGIC     dl.NOME ,
# MAGIC     dl.CIDADE ,
# MAGIC     dl.BAIRRO ,
# MAGIC     dl.ESTADO ,
# MAGIC     dl.Insert_Date
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabelas - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select * from gold.dim_local

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC   delta.`/mnt/gold/dim_local`