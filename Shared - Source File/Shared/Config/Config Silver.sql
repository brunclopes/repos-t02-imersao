-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS silver
LOCATION '/mnt/silver/db/'

-- COMMAND ----------

CREATE TABLE silver.artista(
	COD_ARTISTA int,
	CPF varchar(11),
	NOME varchar(50),
	COD_ESTILO int,
    Updated_Date varchar(50),
    Insert_Date varchar(50),
    Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/artista'
COMMENT 'tabela artistas' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.estilo_musical(
  COD_ESTILO INT,
  DESCRICAO VARCHAR(30),
  Updated_Date varchar(50),
  Insert_Date varchar(50),
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/estilo_musical'
COMMENT 'tabela de estilos musicais' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.evento
(
  COD_EVENTO INT,
  NOME VARCHAR(50),
  DATA_EVENTO TIMESTAMP,
  FKCOD_LOCAL INT,
  FKCOD_ORGANIZADOR INT,
  Updated_Date varchar(50),
  Insert_Date varchar(50),
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/evento'
COMMENT 'tabela de eventos' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.evento_artista
(
  CODIGO INT,
  VALOR_PGTO DOUBLE,
  DT_PGTO TIMESTAMP,
  FKCOD_ARTISTA INT,
  FKCOD_EVENTO INT,
  Updated_Date varchar(50),
  Insert_Date varchar(50),
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/evento_artista'
COMMENT 'tabela de evento_artista' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.localizacao(
  COD_LOCAL INT,
  NOME VARCHAR(50) NOT NULL,
  CIDADE VARCHAR(100),
  BAIRRO VARCHAR(30),
  ESTADO VARCHAR(2),
  Updated_Date varchar(50),
  Insert_Date varchar(50),
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/localizacao'
COMMENT 'tabela de localizacao' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.organizador
(
  MATRICULA INT,
  NOME VARCHAR(50),
  Updated_Date varchar(50),
  Insert_Date varchar(50),
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/organizador'
COMMENT 'tabela de organizador' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.telefone(
  COD_TEL INT,
  NUMERO VARCHAR(30),
  MAT_ORGANIZADOR INT,
  Updated_Date varchar(50),
  Insert_Date varchar(50),
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db/telefone'
COMMENT 'tabela de telefones' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

truncate table silver.artista;
truncate table silver.estilo_musical;
truncate table silver.evento;
truncate table silver.evento_artista;
truncate table silver.localizacao;
truncate table silver.organizador;
truncate table silver.telefone;