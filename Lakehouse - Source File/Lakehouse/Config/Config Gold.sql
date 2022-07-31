-- Databricks notebook source
CREATE DATABASE gold
LOCATION '/mnt/Gold/'

-- COMMAND ----------

CREATE TABLE gold.dim_calendario(
	ANO VARCHAR(4)
	, MES VARCHAR(2)
	, DIA VARCHAR(2)
	, DATA DATE
)
USING DELTA
LOCATION '/mnt/gold/dim_calendario'
COMMENT 'dimensão de calendário' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

-- OK

CREATE TABLE gold.dim_artista(
	COD_ARTISTA INT
	, CPF VARCHAR(11)
	, NOME VARCHAR(100)
	, DESCRICAO VARCHAR(50)
    , Insert_Date VARCHAR(20)
    , Updated_Date VARCHAR(20)
)
USING DELTA
LOCATION '/mnt/gold/dim_artista'
COMMENT 'dimensão de artistas' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

-- OK

CREATE TABLE gold.dim_local(
	COD_LOCAL INT
	, NOME VARCHAR(255)
	, CIDADE VARCHAR(50)
	, BAIRRO VARCHAR(50)
	, ESTADO VARCHAR(50)
    , Insert_Date VARCHAR(20)
    , Updated_Date VARCHAR(20)
)
USING DELTA
LOCATION '/mnt/gold/dim_local'
COMMENT 'dimensão de localização' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE gold.dim_organizador(
	MATRICULA INT
	, NOME VARCHAR(50)
	, NUMERO VARCHAR(15)
    , Insert_Date VARCHAR(20)
    , Updated_Date VARCHAR(20)
)
USING DELTA
LOCATION '/mnt/gold/dim_organizador'
COMMENT 'dimensão de organizador' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

DROP TABLE IF EXISTS gold.fato_evento;

CREATE TABLE gold.fato_evento(
	COD_EVENTO INT
	, COD_LOCAL INT
	, NOME VARCHAR(50)
	, DT_EVENTO DATE
	, HR_EVENTO VARCHAR(20)
	, COD_ORGANIZADOR INT
	, COD_ARTISTA INT
	, VALOR_PGTO DOUBLE
	, DT_PGTO DATE
    , Insert_Date VARCHAR(20)
    , Updated_Date VARCHAR(20)
)
USING DELTA
LOCATION '/mnt/gold/fato_evento'
COMMENT 'fato evento' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');