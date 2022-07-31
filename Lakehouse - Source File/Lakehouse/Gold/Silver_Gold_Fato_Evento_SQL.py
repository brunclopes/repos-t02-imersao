# Databricks notebook source
# DBTITLE 1,Importanto bibliotecas
from datetime import date

# COMMAND ----------

today = date.today()

data_formatada = today.strftime("%Y%m%d")

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
silver_path_evento = f"/mnt/silver/evento/{data_formatada}/"
silver_path_evento_artista = f"/mnt/silver/evento_artista/{data_formatada}/"

#Diretorio de destino
gold_path_SQL = "/mnt/gold/fato_evento/"

#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.silver_path_evento', silver_path_evento)
spark.conf.set('var.silver_path_evento_artista', silver_path_evento_artista)
spark.conf.set('var.gold_path_SQL', gold_path_SQL)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela evento - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_eventoSQL USING parquet
# MAGIC OPTIONS (path '${var.silver_path_evento}')

# COMMAND ----------

# DBTITLE 1,Leitura da tabela evento_artista - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_evento_artistaSQL USING parquet
# MAGIC OPTIONS (path '${var.silver_path_evento_artista}')

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
# MAGIC     , Insert_Date
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
# MAGIC   , eva.Insert_Date
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
# MAGIC SELECT distinct
# MAGIC   COD_EVENTO
# MAGIC   , FKCOD_LOCAL as COD_LOCAL
# MAGIC   , NOME
# MAGIC   , date_format(DATA_EVENTO, 'yyyy-MM-dd') as DT_EVENTO
# MAGIC   , date_format(DATA_EVENTO, 'HH:mm:ss') as HR_EVENTO
# MAGIC   , FKCOD_ORGANIZADOR as COD_ORGANIZADOR
# MAGIC   , FKCOD_ARTISTA as COD_ARTISTA
# MAGIC   , VALOR_PGTO
# MAGIC   , date_format(DT_PGTO, 'yyyy-MM-dd') as DT_PGTO
# MAGIC   , Insert_Date
# MAGIC FROM tempView_evento_artistaJoinSQL

# COMMAND ----------

# DBTITLE 1,Consultando tempview após transformaçã - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_Fato_EventoRmDupSQL
# MAGIC AS
# MAGIC SELECT
# MAGIC   ROW_NUMBER() OVER(PARTITION BY COD_EVENTO, COD_LOCAL, COD_ORGANIZADOR, COD_ARTISTA ORDER BY Insert_Date DESC) as Dup
# MAGIC   , COD_EVENTO
# MAGIC   , COD_LOCAL
# MAGIC   , NOME
# MAGIC   , DT_EVENTO
# MAGIC   , HR_EVENTO
# MAGIC   , COD_ORGANIZADOR
# MAGIC   , COD_ARTISTA
# MAGIC   , VALOR_PGTO
# MAGIC   , DT_PGTO
# MAGIC   , Insert_Date
# MAGIC FROM tempView_Fato_EventoSQL

# COMMAND ----------

# DBTITLE 1,Gravando dados na tabela fato - SQL
# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO gold.fato_evento as tb
# MAGIC USING tempView_Fato_EventoRmDupSQL as dl
# MAGIC   ON tb.COD_EVENTO = dl.COD_EVENTO
# MAGIC   AND tb.COD_LOCAL = dl.COD_LOCAL
# MAGIC   AND tb.COD_ORGANIZADOR = dl.COD_ORGANIZADOR
# MAGIC   AND tb.COD_ARTISTA = dl.COD_ARTISTA
# MAGIC   AND dl.Dup = 1
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   tb.NOME = dl.NOME ,
# MAGIC   tb.DT_EVENTO = dl.DT_EVENTO ,
# MAGIC   tb.HR_EVENTO = dl.HR_EVENTO ,
# MAGIC   tb.VALOR_PGTO = dl.VALOR_PGTO ,
# MAGIC   tb.Updated_Date = date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC   (
# MAGIC     COD_EVENTO ,
# MAGIC     COD_LOCAL ,
# MAGIC     NOME ,
# MAGIC     DT_EVENTO ,
# MAGIC     HR_EVENTO ,
# MAGIC     COD_ORGANIZADOR ,
# MAGIC     COD_ARTISTA ,
# MAGIC     VALOR_PGTO ,
# MAGIC     DT_PGTO ,
# MAGIC     Insert_Date
# MAGIC   )
# MAGIC   VALUES
# MAGIC   (
# MAGIC     dl.COD_EVENTO ,
# MAGIC     dl.COD_LOCAL ,
# MAGIC     dl.NOME ,
# MAGIC     dl.DT_EVENTO ,
# MAGIC     dl.HR_EVENTO ,
# MAGIC     dl.COD_ORGANIZADOR ,
# MAGIC     dl.COD_ARTISTA ,
# MAGIC     dl.VALOR_PGTO ,
# MAGIC     dl.DT_PGTO ,
# MAGIC     dl.Insert_Date
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from gold.fato_evento

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC   delta.`/mnt/gold/fato_evento`