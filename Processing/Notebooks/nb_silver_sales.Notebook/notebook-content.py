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
from pyspark.sql import Window as W
from pyspark.sql import functions as F

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


df_sales_orders = (
    df["SalesOrderHeader"]
    .select(
        "SalesOrderID",
        "CustomerID",
        "ShipToAddressID",  # Requer tabela Address
        "BillToAddressID",  # Requer tabela Address
        F.to_date("OrderDate").alias("OrderDate"),
        F.to_date("DueDate").alias("DueDate"),
        F.to_date("ShipDate").alias("ShipDate"),
        "SalesOrderNumber",
        "PurchaseOrderNumber",
        "AccountNumber",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
        F.to_date("ModifiedDate").alias("OrderModifiedDate") )
)

df_sales_details = (
    df["SalesOrderDetail"]
    .select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "ProductID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        F.to_date("ModifiedDate").alias("DetailModifiedDate")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Criação do Dataframe principal

# CELL ********************

# Criação do Dataframe principal


# Criação do Dataframe principal
df_join = (
    df_sales_details
    .join(df_sales_orders, "SalesOrderID", how = "inner" )
)

# Adiciona colunas de controle
df_join = (
    df_join
    .withColumn("ModifiedDate", F.greatest(
        F.col("OrderModifiedDate"),
        F.col("DetailModifiedDate") )
    )
    .withColumn("UpdatedDate", F.current_date() )
)

# Remove colunas desnecessárias
df_join = (
    df_join.drop(
        "OrderModifiedDate",
        "DetailModifiedDate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Particionamento pelo cabeçalho (Header)

# CELL ********************

# Particionamento pelo cabeçalho (Header)

window = W.partitionBy("SalesOrderID")

# Criação do Dataframe final
df_final = df_join.withColumn(
    "OrderQtyTotal",
    F.sum("OrderQty").over(window)
)

df_final = (
    df_final
    .select(
        # Colunas de IDs
        "SalesOrderID",
        "SalesOrderDetailID",
        "ProductID",
        "CustomerID",
        "ShipToAddressID",
        "BillToAddressID",
        # Colunas de Datas
        "OrderDate",
        "DueDate",
        "ShipDate",
        # Dimensões Degeneradas
        "SalesOrderNumber",
        "PurchaseOrderNumber",
        "AccountNumber",
        # Colunas de Valores Originais
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
        # Colunas de Valores Rateados
        ( F.col("SubTotal") * F.col("OrderQty") / F.col("OrderQtyTotal") ).alias("SubTotalRatio"),
        ( F.col("TaxAmt") * F.col("OrderQty") / F.col("OrderQtyTotal") ).alias("TaxAmtRatio"),
        ( F.col("Freight") * F.col("OrderQty") / F.col("OrderQtyTotal") ).alias("FreightRatio"),
        ( F.col("TotalDue") * F.col("OrderQty") / F.col("OrderQtyTotal") ).alias("TotalDueRatio"),
        # Colunas de Controle
        "ModifiedDate",
        "UpdatedDate"
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
