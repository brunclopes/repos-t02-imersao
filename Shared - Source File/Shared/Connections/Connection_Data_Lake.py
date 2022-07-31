# Databricks notebook source
# DBTITLE 1,Estabelecendo conexão com o data lake
config = {"fs.azure.account.key.dlsestudosbruno.blob.core.windows.net":dbutils.secrets.get(scope = "scopebrickstreinamentobruno", key = "kvkeydatalake")}

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
                source = f"wasbs://{container}@dlsestudosbruno.blob.core.windows.net",
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
# MAGIC ls /mnt/bronze

# COMMAND ----------

mount_datalake(containers)

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
dbutils.fs.ls ("/mnt/bronze")

# COMMAND ----------

# DBTITLE 1,Validação Key Vault
keydatalake = "HJHthyuiop245#$%¨&*()_"
print(keydatalake)

keydatalake = dbutils.secrets.get(scope = "scopebrickstreinamentobruno", key = "kvkeydatalake")
print("teste/"+keydatalake)