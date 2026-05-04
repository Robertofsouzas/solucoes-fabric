# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ####  Configurações da sessão spark

# CELL ********************

%run nb_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# configurações da sessão spark
spark.conf.set("spark.sql.casesensitive",True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Bibliotecas Necessárias 

# CELL ********************

# Bibliotecas Necessárias 
import sempy.fabric as fabric
from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Obtenção do wokspace id e workspace name

# CELL ********************

# Obtenção do wokspace id e do workspace name

workspace_id  = fabric.get_notebook_workspace_id()
workspace_name = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Parâmetros passados pelo pipeline ( Obtidos a partir do Orquestrador)

# PARAMETERS CELL ********************

# Parâmetros passados pelo pipeline ( Obtidos a partir do Orquestrador)
source_storage = ""
source_tables  = ""


target_storage = ""
target_table   = ""
target_mode    = ""   # opção : overwrite , Append e Merge

target_key    = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Caminho absoluto para o destino ( tabela delta)

# CELL ********************

# Caminho absoluto para o destino ( tabela delta)

path_silver = (
    f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{target_storage}.Lakehouse/Tables/dbo/{target_table}"

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Leitura das tabelas

# CELL ********************

# Leitura das tabelas

# Split das tabelas em lista para interação
source_tables = source_tables.split("|")
df={}


# Iterador
for table in source_tables:
    path_bronze = (
        f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{source_storage}.Lakehouse/Tables/dbo/{table}"
    )
    # Faz a leitura e armazena cada tabela em seu respectivo Dataframe
    df[table] = spark.read.format("delta").load(path_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### seleção de colunas e linhas

# CELL ********************

# seleção de colunas e linhas
df_address = ( 
    df['Address']
    .select(
         "AddressID",
           "AddressLine1",
            "AddressLine2",
            "City",
            "StateProvince",
            "CountryRegion",
            "PostalCode", 
            f.to_date("ModifiedDate" ).alias("AddressModifiedDate"))

)

df_customer = (
    df['Customer']
    .select(
        "CustomerID",
        "Title",
        "FirstName",
        "MiddleName",
        "LastName",
        "Suffix",
        "CompanyName",
        "SalesPerson",
        "EmailAddress",
        f.to_date("ModifiedDate").alias("CustomerModifiedDate")
    )
)

df_bridge = (
    df['CustomerAddress']
    .select(
        "CustomerID",
        "AddressID",
        f.to_date("ModifiedDate").alias("BridgeModifiedDate")
    )
    .where(f.col("AddressType")== "Main Office")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bridge)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criação do Dataframe principal


df_final = (
    df_customer
    .join(df_bridge, "CustomerID", how= "left")
    .join(df_address, "AddressID", how= "left")
)


# Adiciona colunas de controle

df_final= (
    df_final
    .withColumn("ModifiedDate", f.greatest(
     f.col("AddressModifiedDate"),
     f.col("CustomerModifiedDate"),
     f.col("BridgeModifiedDate"))
     
     )
      .withColumn("UpdateDate",f.current_date())
)

# Criando a nova coluna 'FullName' mesclando as quatro colunas
df_final = df_final.withColumn(
    "FullName", 
    f.concat_ws(" ", f.col("Title"), f.col("FirstName"), f.col("MiddleName"), f.col("LastName"))
)

# Remove colunas desnecessárias
df_final=(
    df_final.drop(
        "AddressID",
        "AddressModifiedDate",
        "CustomerModifiedDate",
        "BridgeModifiedDate",
        "Title",
        "FirstName",
        "MiddleName",
        "LastName"
        )

    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Gravação da tabela de acordo com o modo atribuído

# CELL ********************

# Gravação da tabela de acordo com o modo atribuído
if target_mode == "merge":
    safe_merge(df_final, path_silver,target_key)

else:
    df_final.write.format("delta").mode(target_mode).save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
