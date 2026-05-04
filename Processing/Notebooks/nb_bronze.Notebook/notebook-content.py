# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "549008b0-e0f5-4e5d-a2da-6357bbbf3694",
# META       "default_lakehouse_name": "lh_Bronze",
# META       "default_lakehouse_workspace_id": "f6ae9386-89e1-4cf6-84d9-3900cd4b69fa",
# META       "known_lakehouses": [
# META         {
# META           "id": "549008b0-e0f5-4e5d-a2da-6357bbbf3694"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Configuraões da sessão spark
spark.conf.set("spark.sql.caseSensitive",True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# obtenção do workspace id e do workspace name

import sempy.fabric as fabric
worspace_id = fabric.get_workspace_id()
workspace_name = fabric.resolve_workspace_name()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parâmetros passados pelo pipeline ( obtidos a partir do orquestrador)

workspace_name= "ws_fabric"
source_storage= "lh_bronze"
source_folder="Staging"
source_file="Customer.parquet"

target_storage= "lh_bronze"
target_table= "customer"
target_mode= "overwrite"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Caminho absoluto para a origem ( arquivos parquet)
path_staging=f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{source_storage}.Lakehouse/Files/{source_folder}/{source_file}"

# Caminho absoluto para o detino (tabelas Deltas)
path_bronze = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{target_storage}.Lakehouse/Tables/dbo/{target_table}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura do arquivo parquet na staging
df = spark.read.parquet(path_staging)


# Escrita da Tabela Delta na camada Bronze
df.write.format("delta").mode(target_mode).save(path_bronze)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
