# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
bronze_path_evento = "/mnt/bronze/evento.csv"
 
#Diretorio de destino
silver_path_deltaTable = "/mnt/silver/db/evento/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_evento', bronze_path_evento)
spark.conf.set('var.silver_path_deltaTable', silver_path_deltaTable)

# COMMAND ----------

# DBTITLE 1,Leitura csv evento - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_eventoSQL USING csv
# MAGIC OPTIONS ('header' = 'true', 'inferSchema' = 'true', sep=';', path '${var.bronze_path_evento}')

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_eventoSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM tempView_eventoSQL

# COMMAND ----------

# DBTITLE 1,Adicionando Campos na tempview + Removendo dados duplicados + Convertendo colunas - evento- SQL
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_evento_newColumns_SQL
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   cast(COD_EVENTO as int) as COD_EVENTO
# MAGIC   , NOME
# MAGIC   , cast(DATA_EVENTO as timestamp) as DATA_EVENTO
# MAGIC   , FKCOD_LOCAL
# MAGIC   , FKCOD_ORGANIZADOR
# MAGIC   , cast(coalesce(null, null, null) as varchar(20)) as Updated_Date
# MAGIC   , date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss') as Insert_Date
# MAGIC FROM tempView_eventoSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  SQL
# MAGIC %sql
# MAGIC 
# MAGIC select * from tempView_evento_newColumns_SQL;

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_evento_newColumns_SQL
# MAGIC LIMIT 2

# COMMAND ----------

# DBTITLE 1,Filtro - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_evento_newColumns_SQL
# MAGIC WHERE
# MAGIC   COD_EVENTO = 2

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   NOME
# MAGIC   , length(NOME) as TAMANHO_NOME
# MAGIC FROM tempView_evento_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,criando campos de controle - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_evento_colunaControleSql
# MAGIC AS
# MAGIC SELECT
# MAGIC   COD_EVENTO
# MAGIC   , NOME
# MAGIC   , DATA_EVENTO
# MAGIC   , FKCOD_LOCAL
# MAGIC   , FKCOD_ORGANIZADOR
# MAGIC   , Updated_Date
# MAGIC   , Insert_Date
# MAGIC   , date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC FROM tempView_evento_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Exibindo schema da tempView - SQL
# MAGIC %sql
# MAGIC 
# MAGIC describe table tempView_evento_colunaControleSql

# COMMAND ----------

# DBTITLE 1,Atualizando coluna da tabela - Updated_Date - SQL
# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver.evento as tb
# MAGIC USING tempView_evento_colunaControleSql as df
# MAGIC ON tb.COD_EVENTO = df.COD_EVENTO
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tb.Updated_Date = date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO silver.evento
# MAGIC SELECT
# MAGIC   COD_EVENTO
# MAGIC   , NOME
# MAGIC   , DATA_EVENTO
# MAGIC   , FKCOD_LOCAL
# MAGIC   , FKCOD_ORGANIZADOR
# MAGIC   , Updated_Date
# MAGIC   , Insert_Date
# MAGIC   , date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC FROM tempView_evento_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela Delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from silver.evento

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC delta.`/mnt/silver/db/evento/`

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC truncate table silver.evento

# COMMAND ----------

