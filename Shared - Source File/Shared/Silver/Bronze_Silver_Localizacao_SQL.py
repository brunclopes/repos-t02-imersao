# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
bronze_path_localizacao = "/mnt/bronze/localizacao.csv"
 
#Diretorio de destino
silver_path_deltaTable = "/mnt/silver/db/localizacao/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_localizacao', bronze_path_localizacao)
spark.conf.set('var.silver_path_deltaTable', silver_path_deltaTable)

# COMMAND ----------

# DBTITLE 1,Leitura csv localizacao - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_localizacaoSQL USING csv
# MAGIC OPTIONS ('header' = 'true', 'inferSchema' = 'true', sep=';', path '${var.bronze_path_localizacao}')

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_localizacaoSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM tempView_localizacaoSQL

# COMMAND ----------

# DBTITLE 1,Adicionando Campos na tempview + Removendo dados duplicados - evento- SQL
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_localizacao_newColumns_SQL
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   COD_LOCAL
# MAGIC   , NOME
# MAGIC   , CIDADE
# MAGIC   , BAIRRO
# MAGIC   , ESTADO
# MAGIC   , cast(coalesce(null, null, null) as varchar(20)) as Updated_Date
# MAGIC   , date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss') as Insert_Date
# MAGIC FROM tempView_localizacaoSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  SQL
# MAGIC %sql
# MAGIC 
# MAGIC select * from tempView_localizacao_newColumns_SQL;

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_localizacao_newColumns_SQL
# MAGIC LIMIT 2

# COMMAND ----------

# DBTITLE 1,Filtro - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_localizacao_newColumns_SQL
# MAGIC WHERE
# MAGIC   COD_LOCAL = 2

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   NOME
# MAGIC   , length(NOME) as TAMANHO_NOME
# MAGIC FROM tempView_localizacao_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,criando campos de controle - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_localizacao_colunaControleSql
# MAGIC AS
# MAGIC SELECT
# MAGIC   COD_LOCAL
# MAGIC   , NOME
# MAGIC   , CIDADE
# MAGIC   , BAIRRO
# MAGIC   , ESTADO
# MAGIC   , Updated_Date
# MAGIC   , Insert_Date
# MAGIC   , date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC FROM tempView_localizacao_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Exibindo schema da tempView - SQL
# MAGIC %sql
# MAGIC 
# MAGIC describe table tempView_localizacao_colunaControleSql

# COMMAND ----------

# DBTITLE 1,Atualizando coluna da tabela - Updated_Date - SQL
# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver.localizacao as tb
# MAGIC USING tempView_localizacao_colunaControleSql as df
# MAGIC ON tb.COD_LOCAL = df.COD_LOCAL
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tb.Updated_Date = date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO silver.localizacao
# MAGIC SELECT
# MAGIC   COD_LOCAL
# MAGIC   , NOME
# MAGIC   , CIDADE
# MAGIC   , BAIRRO
# MAGIC   , ESTADO
# MAGIC   , Updated_Date
# MAGIC   , Insert_Date
# MAGIC   , date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC FROM tempView_localizacao_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela Delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from silver.localizacao

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC delta.`/mnt/silver/db/localizacao/`

# COMMAND ----------

# DBTITLE 1,Truncando Tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC truncate table 
# MAGIC delta.`/mnt/silver/db/localizacao/`