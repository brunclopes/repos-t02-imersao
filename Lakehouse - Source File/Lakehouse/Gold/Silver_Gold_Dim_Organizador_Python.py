# Databricks notebook source
# DBTITLE 1,Importando bibliotecas - Python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import date

# COMMAND ----------

today = date.today()

data_formatada = today.strftime("%Y%m%d")

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de destino 
gold_path_Python = "/mnt/gold/dim_organizador/"


#Diretorio de origem
silver_path_organizador = f"/mnt/silver/organizador/{data_formatada}/"
silver_path_telefone = f"/mnt/silver/telefone/{data_formatada}/"

#Diretorio de destino
gold_path_deltaTable = "/mnt/gold/dim_organizador/"

# COMMAND ----------

# DBTITLE 1,Leitura da tabela organizador - Python
organizadorDF = spark.read.format('parquet').load(silver_path_organizador)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela telefone - Python
telefoneDF = spark.read.format('parquet').load(silver_path_telefone)

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

# DBTITLE 1,Agrupa e concatena numeros de telefone - Python
telefone_clctDF = telefoneDF.groupby("MAT_ORGANIZADOR").agg(concat_ws(" | ", collect_list(telefoneDF.NUMERO)).alias('NUMERO'))

# COMMAND ----------

# DBTITLE 1,Consultado dados atualizados de telefone - Python
display(telefone_clctDF)

# COMMAND ----------

# DBTITLE 1,Join - Python
organizador_joinDF = organizadorDF.join(telefone_clctDF, organizadorDF.MATRICULA == telefone_clctDF.MAT_ORGANIZADOR, 'inner').select(organizadorDF.MATRICULA, organizadorDF.NOME, telefone_clctDF.NUMERO, organizadorDF.Insert_Date)
display(organizador_joinDF)

# COMMAND ----------

# DBTITLE 1,Inserindo dados na tabela - Python
dim_organizadorDF = DeltaTable.forPath(spark, gold_path_deltaTable)

dim_organizadorDF.alias('tb')\
    .merge(
        organizador_joinDF.alias('df'),
        'tb.MATRICULA = df.MATRICULA'
    )\
    .whenMatchedUpdate(set=
        {
           "tb.Updated_Date": date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss') ,
            "tb.NOME": "df.NOME" ,
            "tb.NUMERO": "df.NUMERO"
        }
    )\
    .whenNotMatchedInsert(values=
        {
            "MATRICULA": "df.MATRICULA" ,
            "NOME": "df.NOME" ,
            "NUMERO": "df.NUMERO" ,
            "Insert_Date": "df.Insert_Date"
        }
     )\
    .execute()

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela - Python
goldDF = spark.table('gold.dim_organizador')

display(goldDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from gold.dim_organizador