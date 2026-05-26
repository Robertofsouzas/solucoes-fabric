# 📊 Soluções Fabric

Um projeto de arquitetura de dados usando **Microsoft Fabric**, implementando o padrão **Medallion Architecture** (Bronze → Silver → Gold) com processamento de dados em **PySpark** e otimização de tabelas **Delta Lake**.

---

## 🎯 O que é este projeto?

Este repositório contém **notebooks Python** parametrizados para construir um pipeline de dados completo em Microsoft Fabric, desde a ingestão bruta até a criação de modelos semânticos para análise e BI.

### Caso de Uso
Processamento e transformação de dados de vendas, produtos e clientes através de múltiplas camadas de refinamento de dados.

---

## 🏗️ Arquitetura

O projeto segue a **Medallion Architecture**:

```
┌─────────────────────────────────────────────┐
│           BRONZE (Bruto)                    │
│  Dados originais em formato Parquet         │
│  - Sales, Products, Customers               │
└──────────────────┬──────────────────────────┘
                   │ (Transformação)
┌──────────────────▼──────────────────────────┐
│           SILVER (Refinado)                 │
│  Dados limpos e preparados em Delta Tables  │
│  - Products, Sales, Customers (tratados)    │
└──────────────────┬──────────────────────────┘
                   │ (Agregação)
┌──────────────────▼──────────────────────────┐
│           GOLD (Consumível)                 │
│  Modelos semânticos para BI e Analytics     │
│  - Dimensões e Fatos otimizadas             │
└─────────────────────────────────────────────┘
```

---

## 📁 Estrutura do Projeto

```
Processing/
├── Notebooks/
│   ├── nb_bronze.Notebook/
│   │   └── notebook-content.py          # Ingestão de arquivos Parquet
│   ├── nb_silver_sales.Notebook/
│   │   └── notebook-content.py          # Transformação de Vendas
│   ├── nb_silver_products.Notebook/
│   │   └── notebook-content.py          # Transformação de Produtos
│   ├── nb_silver_customers.Notebook/
│   │   └── notebook-content.py          # Transformação de Clientes
│   ├── nb_functions.Notebook/
│   │   └── notebook-content.py          # Funções reutilizáveis (Merge, Validação)
│   ├── nb_semantic_models.Notebook/
│   │   └── notebook-content.py          # Configuração de modelos semânticos
│   └── nb_delta_optimization.Notebook/
│       └── notebook-content.py          # Otimização de tabelas Delta
```

---

## 🔄 Fluxo de Processamento

### 1️⃣ **BRONZE** - Ingestão Bruta
**Arquivo:** `nb_bronze.Notebook`

- Lê arquivos **Parquet** da staging area
- Salva diretamente em tabelas Delta na camada Bronze
- **Modo padrão:** Sobrescrever dados

```python
df = spark.read.parquet(path_staging)
df.write.format("delta").mode(target_mode).save(path_bronze)
```

### 2️⃣ **SILVER** - Transformação e Limpeza
**Arquivos:** `nb_silver_sales.Notebook`, `nb_silver_products.Notebook`, `nb_silver_customers.Notebook`

- Lê múltiplas tabelas da camada Bronze
- Aplica transformações e lógica de negócio
- Usa a função `safe_merge()` para atualizar dados de forma segura
- **Modos suportados:** Overwrite, Append, Merge

#### Exemplo de Merge:
```python
def safe_merge(source, path, Key):
    if table_exists(path):
        # Atualiza registros existentes e insere novos
        target.merge(source, condition=f"target.{Key} = source.{Key}") \
            .whenMatchedUpdate(condition="source.ModifiedDate > target.ModifiedDate") \
            .whenNotMatchedInsert(values=update_cols) \
            .execute()
    else:
        # Primeira carga: sobrescrever
        source.write.format("delta").mode("overwrite").save(path)
```

### 3️⃣ **GOLD** - Modelos Semânticos
**Arquivo:** `nb_semantic_models.Notebook`

- Cria modelos semânticos para BI usando **DirectLake**
- Conecta tabelas Delta diretamente aos modelos
- Otimiza para análise em Power BI

```python
directlake.update_direct_lake_model_connection(
    dataset="sm_solucoes-fabric_composite",
    source="lh_gold",
    tables=["DimCustomers", "DimProducts", "FactSales"]
)
```

### 4️⃣ **OTIMIZAÇÃO** - Manutenção de Tabelas
**Arquivo:** `nb_delta_optimization.Notebook`

- **Analyze:** Coleta métricas de performance
- **Optimize:** Reorganiza dados e compacta partições
- **Vacuum:** Remove arquivos antigos
- **V-Order:** Otimização de leitura por coluna

```python
lake.run_table_maintenance(
    table_name="dbo/Sales",
    optimize=True,
    v_order=True,
    vacuum=True
)
```

---

## 📋 Notebooks Detalhados

| Notebook | Responsabilidade | Entrada | Saída |
|----------|------------------|---------|-------|
| **nb_bronze** | Ingestão de dados brutos | Arquivos Parquet (staging) | Tabelas Delta (Bronze) |
| **nb_silver_sales** | Transformação de vendas | Tabelas Bronze | Tabelas Silver (Sales) |
| **nb_silver_products** | Transformação de produtos | Tabelas Bronze | Tabelas Silver (Products) |
| **nb_silver_customers** | Transformação de clientes | Tabelas Bronze | Tabelas Silver (Customers) |
| **nb_functions** | Funções auxiliares | - | Funções `table_exists()`, `safe_merge()` |
| **nb_semantic_models** | Modelos para BI | Tabelas Gold | Modelos Semânticos (DirectLake) |
| **nb_delta_optimization** | Otimização Delta | Tabelas lakehouse | Tabelas otimizadas |

---

## 🔧 Parâmetros de Entrada

Todos os notebooks aceitam parâmetros do **orquestrador** (pipeline):

### Bronze
```python
source_storage = "storage_origin"      # Lakehouse de origem
source_folder = "folder_name"          # Pasta no storage
source_file = "file_name.parquet"      # Arquivo a processar
target_storage = "target_lakehouse"    # Lakehouse destino
target_table = "table_name"            # Nome da tabela Delta
target_mode = "overwrite"              # Modo de escrita
```

### Silver
```python
source_storage = "lh_bronze"           # Lakehouse de origem
source_tables = "Table1|Table2|Table3" # Tabelas (separadas por |)
target_storage = "lh_silver"           # Lakehouse destino
target_table = "Sales"                 # Tabela Silver
target_mode = "merge"                  # overwrite | append | merge
target_key = "SalesID"                 # Chave para merge
```

### Semantic Models
```python
semantic_model = "sm_solucoes-fabric_composite"  # Nome do modelo
source_storage = "lh_gold"                       # Lakehouse
tables = "DimCustomers;DimProducts;FactSales"   # Tabelas (separadas por ;)
flag = True                                      # Aplicar a tabelas específicas
```

---

## 🚀 Como Usar

### Pré-requisitos
- ✅ Microsoft Fabric ativo
- ✅ PySpark disponível
- ✅ Lakehouse configurado
- ✅ Dados de origem em formato Parquet

### Execução
1. **Configure os parâmetros** em cada notebook
2. **Execute o pipeline** na sequência:
   - `nb_bronze` → Ingestão
   - `nb_silver_*` → Transformações
   - `nb_semantic_models` → Modelos de BI
   - `nb_delta_optimization` → Otimização

### Orquestração
Recomenda-se usar **Data Factory** ou **Pipelines Fabric** para orquestrar automaticamente os notebooks com parâmetros dinâmicos.

---

## 🎓 Conceitos-Chave

### Delta Lake
- Tabelas ACID com versionamento
- Otimizadas para análise de big data
- Suportam merge/update/delete

### DirectLake
- Conexão direta entre Lakehouse e modelos semânticos
- Zero latência no Power BI
- Melhor performance que Import

### PySpark
- Processamento distribuído de dados
- SQL + DataFrame APIs
- Executado em clusters Fabric

### Medallion Architecture
- **Bronze:** Dados brutos como-estão
- **Silver:** Dados limpos e normalizados
- **Gold:** Dados agregados para consumo

---

## 📊 Exemplos de Transformações

### Merge de Dados (Silver)
```python
# Atualiza vendas apenas se houver mudanças recentes
(
    target.alias("target")
    .merge(source.alias("source"), 
           condition="target.SalesID = source.SalesID")
    .whenMatchedUpdate(
        condition="source.ModifiedDate > target.ModifiedDate",
        set={"Amount": "source.Amount", "Status": "source.Status"})
    .whenNotMatchedInsert(
        values={"SalesID": "source.SalesID", "Amount": "source.Amount"})
    .execute()
)
```

### Análise de Tabelas Delta
```python
# Gera relatório de saúde da tabela
x = labs.delta_analyzer(
    table_name="dbo/Sales",
    lakehouse="lh_silver",
    workspace=workspace
)
# Retorna: fragmentação, tamanho, versões, etc.
```

---

## 📈 Performance

### Otimizações Incluídas
- ✅ **V-Order:** Compressão e ordenação otimizada
- ✅ **Optimize:** Compactação de pequenos arquivos
- ✅ **Vacuum:** Limpeza de histórico transacional
- ✅ **Case-Sensitive:** Configuração spark para precisão

### Benefícios
- 🚀 Queries mais rápidas
- 💾 Menor consumo de armazenamento
- 🔄 Melhor performance em merges

---

## 🤝 Contribuição

Para contribuir com melhorias:
1. Crie um branch para sua feature
2. Envie um pull request com detalhes
3. Mantenha a estrutura Medallion

---

## 📝 Licença

Este projeto é fornecido como-está para fins educacionais e comerciais.

---

## 📞 Suporte

Para dúvidas sobre implementação:
- Consulte a [documentação Fabric](https://learn.microsoft.com/fabric/)
- Veja exemplos de [DirectLake](https://learn.microsoft.com/power-bi/connect-data/directlake-overview)
- Estude [Delta Lake optimization](https://learn.microsoft.com/en-us/fabric/onelake/delta-lake-table-optimization)

---

**Versão:** 1.0  
**Linguagem:** Python 3.x + PySpark  
**Plataforma:** Microsoft Fabric  
**Última atualização:** 2026-05-26
