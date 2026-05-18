# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# instalação do semantic Labs
%pip install semantic-link-labs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bibliotecas Necessárias
import sempy.fabric as fabric

import sempy_labs as labs
import sempy_labs.lakehouse as lake

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# obtençao do workspace
workspace = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parametros

lakehouse = "lh_silver"
table  = "Sales"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Captura as analises da tabela Delta e armazena em Dataframes

x = labs.delta_analyzer(
    table_name =  table,
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Exibe os resultados do Delta Aanalyzer
for name,df in x.items():
    print(name)
    display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Executa Optimize em todas as tabelas do lakehouse

lake.optimize_lakehouse_tables(
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executa vacuum em todas as tabelas do lakehouse

lake.vacuum_lakehouse_tables(
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executa todas as otimizações Delta para uma tabela especifica
lake.run_table_maintenance(
    table_name = table,
    optimize= True,
    v_order= True,
    schema = "dbo",
    vacuum= True,
    lakehouse = lakehouse,
    workspace = workspace
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
