# Databricks notebook source
# DBTITLE 1,Carrega conexão com o banco de dados - Python
# MAGIC %run
# MAGIC 
# MAGIC /Shared/Connections/Connections_DB

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
gold_path = "/mnt/gold/dim_organizador/"
 
#Nome da tabela de destino
tb_name = "DW.DIM_ORGANIZADOR"

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos da dim_local - Python
dim_organizadorDF = spark.read.format("parquet").load(gold_path)

# COMMAND ----------

# DBTITLE 1,Consulta dados do datagrame - Python
display(dim_organizadorDF)

# COMMAND ----------

# DBTITLE 1,Carrega os dados na tabela no modo append
'''
obs1: Caso a tabela não exista no schema ela será criada
obs2: no modo append os dados são inseridos em apagar os dados que já existem, correndo o risco de duplicar os dados
'''

write_db_append(dim_organizadorDF, tb_name)

# COMMAND ----------

# DBTITLE 1,Carrega os dados na tabela no modo overwrite
'''
obs1: Caso a tabela não exista no schema ela será criada
obs2: Caso a tabela já exista com outra tipagem, a função retornará um erro pois o comando overwrite apenas apaga os dados e reincere, não recria a tabela
'''

write_db_owerwrite(dim_organizadorDF, tb_name)

# COMMAND ----------

# DBTITLE 1,Consulta os dados da tabela no banco
display(read_db_table('DW.DIM_ORGANIZADOR'))

# COMMAND ----------

# DBTITLE 1,Consulta os dados da tabela no banco baseada em uma query
display(read_db_query('SELECT TOP 1 * FROM DW.DIM_ORGANIZADOR'))