# Databricks notebook source
# DBTITLE 1,Estabelecendo conexão com o data lake
config = {"fs.azure.account.key.dlslakehousebruno.blob.core.windows.net":dbutils.secrets.get(scope = "scopebrickstreinamentobruno", key = "dbw-lakehouse-key")}

# COMMAND ----------

# DBTITLE 1,Lista de containers
containers = ["bronze", "silver", "gold"]

# COMMAND ----------

# DBTITLE 1,Criar ponto de montagem nas camadas
def mount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.mount(
                #https://sttreinamento01.dfs.core.windows.net/
                source = f"wasbs://{container}@dlslakehousebruno.blob.core.windows.net",
                mount_point = f"/mnt/{container}",
                extra_configs = config
            )
    except ValueError as err:
        print(err)

# COMMAND ----------

# DBTITLE 1,Desmonta camadas do DBFS - Alerta (Apaga camadas)
def unmount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.unmount(f"/mnt/{container}/")
    except ValueError as err:
        print(err)

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/

# COMMAND ----------

mount_datalake(containers)

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
dbutils.fs.ls ("/mnt/")

# COMMAND ----------

# DBTITLE 1,Validação Key Vault
dbwlakehousekey = "HJHthyuiop245#$%¨&*()_"
print(dbwlakehousekey)

dbwlakehousekey = dbutils.secrets.get(scope = "scopebrickstreinamentobruno", key = "dbw-lakehouse-key")
print("teste/"+dbwlakehousekey)