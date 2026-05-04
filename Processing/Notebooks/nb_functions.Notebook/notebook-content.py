# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Função para validar existencia de tabelas delta
def table_exists(path: str) -> bool:
 from pyspark.sql.utils import AnalysisException
 try:
    spark.read.format("delta").load(path)
    return True
 except AnalysisException:
        return False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Funçao para executar o merge de forma segura

def safe_merge(source,path, Key):

    if table_exists(path_silver):
        # Executar o merge
        print("Tabela encontrada, merge em execução")
        from delta.tables import DeltaTable
        
        # variáveis necessárias ao merge
        target = DeltaTable.forPath(spark, path_silver)
        columns = source.columns
        update_cols = {col_name: f"source.{col_name}" for col_name in columns}
        update_condition = "source.ModifiedDate > target.ModifiedDate"
        merge_condition = f"target.{Key} = source.{Key}"

        # instrução Merge

        (
            target.alias("target")
            .merge(source=  source.alias("source"),condition= merge_condition)
            .whenMatchedUpdate(condition= update_condition , set =update_cols)
            .whenNotMatchedInsert(values= update_cols)
            .execute()
        )

    else:
        # Executar o overwrite
        source.write.format("delta").mode("overwrite").save(path_silver)    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
