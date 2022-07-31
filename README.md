## repos-t02-imersao

Repositório da imersão em Azure Big Data, contendo os códigos do ADF (Azure Data Factory), e também os códigos do Databricks. No treinamento foi abordado uma solução de Big Data na Azure, utlizando o Azure Data Factory para ingestão de dados e orquestração de pipelines, o Azure Databricks para ingestão e processamento de dados, o Azure Analysis Services para criação de cubos OLAP, e o Power BI para visualização.
Com o Azure Data Factory, foi trabalhado com as seguintes fontes: SQL Server On Premises e arquivos csv, txt e xlsx via máquina local. Todos os dados foram carregados no Storage da Azure. Pelo Data Lake foram carregados os dados nas camadas raw, trusted e refined, e também em um banco de dados Azure SQL.
Com o Databricks, foi trabalhado com o conceito de tabelas Delta,  navegando pelas camadas raw, silver e gold. Na camada gold as tabelas foram organizadas com o modelo de dimensões e fatos.
Todos os serviços utilizaram chaves criptografadas, a Key Vault, proporcionando maior segurança e simulando 100% um ambiente de produção.
