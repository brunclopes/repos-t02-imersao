# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
bronze_path_artista = "/mnt/bronze/artista.csv"
 
#Diretorio de destino
silver_path_deltaTable = "/mnt/silver/db/artista/"
silver_path_parquetFiles = "/mnt/silver/artista/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.bronze_path_artista', bronze_path_artista)
spark.conf.set('var.silver_path_deltaTable', silver_path_deltaTable)
spark.conf.set('var.silver_path_parquetFiles', silver_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Leitura csv artista - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_artistaSQL USING csv
# MAGIC OPTIONS ('header' = 'true', 'inferSchema' = 'false', sep=';', path '${var.bronze_path_artista}')

# COMMAND ----------

# DBTITLE 1,Leitura csv artista - Python
artistaDF = spark.read.csv(bronze_path_artista, header=True, sep=';', inferSchema=False)
artistaDF.createOrReplaceTempView("tempView_artistaPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
artistaDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_artistaSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
artistaDF.show()
#ou
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
tempView_artistaPythonDF = spark.sql("select * from tempView_artistaPython")
#tempView_artistaPythonDF = spark.table("tempView_artistaPython")

display(tempView_artistaPythonDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM tempView_artistaSQL

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas - python
artistaDF = artistaDF.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Python
artistaDF = artistaDF.withColumn('Updated_Date', lit(''))
artistaDF = artistaDF.withColumn('Insert_Date', date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'))

# COMMAND ----------

# DBTITLE 1,Aplicando tipagem nas colunas de código
#Converte as colunas COD_ESTILO e COD_ARISTA paa int
artistaDF = artistaDF.withColumn("COD_ARTISTA",artistaDF.COD_ARTISTA.cast(IntegerType()))
artistaDF = artistaDF.withColumn("COD_ESTILO",artistaDF.COD_ESTILO.cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Recriando tempview com as novas colunas - Python
artistaDF.createOrReplaceTempView("tempView_artistaPython")

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python
display(spark.sql("select * from tempView_artistaPython"))

# COMMAND ----------

# DBTITLE 1,Adicionando Campos na tempview + Removendo dados duplicados + Convertendo colunas - artista - SQL
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_artista_newColumns_SQL
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   cast(COD_ARTISTA as int) as COD_ARTISTA
# MAGIC   , CPF
# MAGIC   , NOME
# MAGIC   , cast(COD_ESTILO as int) as COD_ESTILO
# MAGIC   , cast(coalesce(null, null, null) as varchar(20)) as Updated_Date
# MAGIC   , date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss') as Insert_Date
# MAGIC FROM tempView_artistaSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  SQL
# MAGIC %sql
# MAGIC 
# MAGIC select * from tempView_artista_newColumns_SQL;

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(artistaDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_artista_newColumns_SQL
# MAGIC LIMIT 2

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(artistaDF.filter(artistaDF.COD_ESTILO == 2))

# COMMAND ----------

# DBTITLE 1,Filtro - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM tempView_artista_newColumns_SQL
# MAGIC WHERE
# MAGIC   COD_ESTILO = 2

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - Python
artistaDF.withColumn('TAMANHO_NOME', length(artistaDF.NOME)).show()
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   NOME
# MAGIC   , length(NOME) as TAMANHO_NOME
# MAGIC FROM tempView_artista_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Adicionando campo de controle - Python
artistaDF = artistaDF.withColumn('Partition_Date', date_format(current_timestamp(), 'yyyyMM'))
artistaDF = artistaDF.withColumn('Partition_Date', artistaDF.Partition_Date.cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Criando campos de controle - SQL
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempView_artista_colunaControleSql
# MAGIC AS
# MAGIC SELECT
# MAGIC   COD_ARTISTA
# MAGIC   , CPF
# MAGIC   , NOME
# MAGIC   , COD_ESTILO
# MAGIC   , Updated_Date
# MAGIC   , Insert_Date
# MAGIC   , cast(date_format(current_timestamp, 'yyyyMM') as int) as Partition_Date
# MAGIC FROM tempView_artista_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Exibindo schema da tempView - SQL
# MAGIC %sql
# MAGIC 
# MAGIC describe table tempView_artista_colunaControleSql

# COMMAND ----------

# DBTITLE 1,Atualizando coluna da tabela - Updated_Date - python
delta_artistaDF = DeltaTable.forPath(spark, silver_path_deltaTable)

delta_artistaDF.alias('tb')\
    .merge(
        artistaDF.alias('df'),
        'tb.COD_ARTISTA = df.COD_ARTISTA'
    )\
    .whenMatchedUpdate(set=
        {
           "tb.Updated_Date": date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')
        }
    )\
    .execute()

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - python
artistaDF.write.mode('append').format('delta').partitionBy('Partition_Date').save(silver_path_deltaTable)

# COMMAND ----------

# DBTITLE 1,Atualizando coluna da tabela - Updated_Date - SQL
# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver.artista as tb
# MAGIC USING tempView_artista_colunaControleSql as df
# MAGIC ON tb.COD_ARTISTA = df.COD_ARTISTA
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tb.Updated_Date = date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss')

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO silver.artista
# MAGIC SELECT
# MAGIC   COD_ARTISTA
# MAGIC   , CPF
# MAGIC   , NOME
# MAGIC   , COD_ESTILO
# MAGIC   , Updated_Date
# MAGIC   , Insert_Date
# MAGIC   , date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC FROM tempView_artista_newColumns_SQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela Delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from silver.artista

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC delta.`/mnt/silver/db/artista/`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC truncate table silver.artista