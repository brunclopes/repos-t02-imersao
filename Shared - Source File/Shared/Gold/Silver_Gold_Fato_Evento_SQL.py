# Databricks notebook source
# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de destino
gold_path_SQL = "/mnt/gold/fato_evento/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.gold_path_SQL', gold_path_SQL)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela evento - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_eventoSQL
# MAGIC AS
# MAGIC SELECT
# MAGIC   COD_EVENTO
# MAGIC   , NOME
# MAGIC   , DATA_EVENTO
# MAGIC   , FKCOD_LOCAL
# MAGIC   , FKCOD_ORGANIZADOR
# MAGIC FROM silver.evento
# MAGIC WHERE 
# MAGIC   Updated_Date IS NULL

# COMMAND ----------

# DBTITLE 1,Leitura da tabela evento_artista - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_evento_artistaSQL
# MAGIC AS
# MAGIC SELECT
# MAGIC   CODIGO
# MAGIC   , VALOR_PGTO
# MAGIC   , DT_PGTO
# MAGIC   , FKCOD_ARTISTA
# MAGIC   , FKCOD_EVENTO
# MAGIC FROM silver.evento_artista
# MAGIC WHERE
# MAGIC   Updated_Date IS NULL

# COMMAND ----------

# DBTITLE 1,Exibir schema da tempview evento - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_eventoSQL;

# COMMAND ----------

# DBTITLE 1,Exibir schema da tempview evento_artista - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_evento_artistaSQL;

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview evento - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_eventoSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview evento_artista - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_evento_artistaSQL

# COMMAND ----------

# DBTITLE 1,Removendo dados duplicados - evento - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_eventoDistinctSQL
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   COD_EVENTO
# MAGIC   , NOME
# MAGIC   , DATA_EVENTO
# MAGIC   , FKCOD_LOCAL
# MAGIC   , FKCOD_ORGANIZADOR
# MAGIC FROM tempView_eventoSQL

# COMMAND ----------

# DBTITLE 1,Removendo dados duplicados - evento_artista - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_evento_artistaDistinctSQL
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC     CODIGO
# MAGIC     , VALOR_PGTO
# MAGIC     , DT_PGTO
# MAGIC     , FKCOD_ARTISTA
# MAGIC     , FKCOD_EVENTO
# MAGIC FROM tempView_evento_artistaSQL

# COMMAND ----------

# DBTITLE 1,Join - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_evento_artistaJoinSQL
# MAGIC AS
# MAGIC SELECT
# MAGIC   ev.COD_EVENTO
# MAGIC   , ev.FKCOD_LOCAL
# MAGIC   , ev.NOME
# MAGIC   , ev.DATA_EVENTO
# MAGIC   , ev.FKCOD_ORGANIZADOR
# MAGIC   , eva.FKCOD_ARTISTA
# MAGIC   , eva.VALOR_PGTO
# MAGIC   , eva.DT_PGTO
# MAGIC FROM tempView_eventoDistinctSQL as ev
# MAGIC INNER JOIN tempView_evento_artistaDistinctSQL as eva
# MAGIC   ON ev.COD_EVENTO = eva.FKCOD_EVENTO

# COMMAND ----------

# DBTITLE 1,Consultando tempview - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_evento_artistaJoinSQL

# COMMAND ----------

# DBTITLE 1,Aplicando transformação nas colunas + adicionando coluna de controle - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_Fato_EventoSQL
# MAGIC AS
# MAGIC SELECT
# MAGIC   COD_EVENTO
# MAGIC   , FKCOD_LOCAL as COD_LOCAL
# MAGIC   , NOME
# MAGIC   , date_format(DATA_EVENTO, 'yyyy-MM-dd') as DT_EVENTO
# MAGIC   , date_format(DATA_EVENTO, 'HH:mm:ss') as HR_EVENTO
# MAGIC   , FKCOD_ORGANIZADOR as COD_ORGANIZADOR
# MAGIC   , FKCOD_ARTISTA as COD_ARTISTA
# MAGIC   , VALOR_PGTO
# MAGIC   , date_format(DT_PGTO, 'yyyy-MM-dd') as DT_PGTO
# MAGIC FROM tempView_evento_artistaJoinSQL

# COMMAND ----------

# DBTITLE 1,Consultando tempview após transformação - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_Fato_EventoSQL

# COMMAND ----------

# DBTITLE 1,Gravando dados na gold - SQL
# MAGIC %sql
# MAGIC 
# MAGIC /*
# MAGIC O comando gera o arquivo na camada gold do data lake, porém, a tabela criada esta no formato delta (delta lake) armazenada no database default
# MAGIC */
# MAGIC 
# MAGIC CREATE external TABLE IF NOT EXISTS table_Fato_EventoSQL
# MAGIC LOCATION '${var.gold_path_SQL}'
# MAGIC as
# MAGIC select *
# MAGIC from tempView_Fato_EventoSQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC   delta.`/mnt/gold/fato_evento`