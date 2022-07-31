# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from datetime import date

# COMMAND ----------

today = date.today()

data_formatada = today.strftime("%Y%m%d")

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
bronze_path_localizacao = "/mnt/bronze/localizacao.csv"
 
#Diretorio de destino
silver_path_parquetFiles = f"/mnt/silver/localizacao/{data_formatada}/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_localizacao', bronze_path_localizacao)
spark.conf.set('var.silver_path_parquetFiles', silver_path_parquetFiles)

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

# DBTITLE 1,Inserindo arquivos parquet na silver - Python
# MAGIC %sql
# MAGIC 
# MAGIC /*
# MAGIC O comando gera o arquivo na camada silver do data lake, porém, a tabela criada esta no formato delta (delta lake) armazenada no database default
# MAGIC */
# MAGIC 
# MAGIC CREATE external TABLE IF NOT EXISTS localizacao
# MAGIC LOCATION '${var.silver_path_parquetFiles}/'
# MAGIC as
# MAGIC select *
# MAGIC from tempView_localizacao_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Exibindo schema da tempView - SQL
# MAGIC %sql
# MAGIC 
# MAGIC describe table tempView_localizacao_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC delta.`${var.silver_path_parquetFiles}`