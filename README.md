# Advanced Techniques with Spark Declarative Pipelines (SDP)

## Visão Geral

Este Repo oferece uma exploração prática do **Lakeflow Spark Declarative Pipelines (SDP)** — o framework moderno da Databricks para a construção de pipelines de streaming de nível de produção. Mostra padrões avançados de design de pipeline, aplicação de qualidade de dados e integração multiplataforma, essenciais para a engenharia de dados em um Lakehouse real.

O Repo inicia com **pipelines Multi-Flow**, ingerindo de múltiplas fontes para uma única tabela de destino usando fluxos explícitos. Em seguida, você explorará o **Liquid Clustering** para otimização adaptativa do layout de dados e **Data Quality Expectations** para aplicar regras de negócio em cada estágio do pipeline.

Na sequência, Mostra o padrão **Multiplex Streaming** para processar fluxos de eventos com esquemas mistos, conectando-se com **Delta Sinks** e **Iceberg Reads via Delta UniForm** para acesso multiplataforma a dados de streaming em tempo real.

O Repo também abrange **Change Data Capture (CDC)** — incluindo **SCD Tipo 1** (estado atual) e **SCD Tipo 2** (rastreamento de histórico completo) — automatizados com o comando `AUTO CDC INTO` no Lakeflow SDP.

Finalmente, implementa técnicas avançadas de qualidade de dados, incluindo restrições baseadas em intervalos, expressões tolerantes a NULL, tratamento de evolução de esquema e o **Padrão de Quarentena (Quarantine Pattern)** para pipelines com zero perda de dados e trilhas de auditoria completas.

O Repo traz teoria e demonstrações práticas de como:

- Construir **pipelines multi-flow** ingerindo dados de múltiplas subsidiárias de varejo em uma única tabela Bronze de streaming.
- Aplicar **Liquid Clustering** e **Data Quality Expectations** em tabelas Silver e Gold.
- Implementar o **padrão Multiplex** com Delta Sinks e Iceberg UniForm para acesso multiplataforma.
- Automatizar o **rastreamento de histórico SCD Tipo 2** usando `AUTO CDC INTO`.
- Projetar **pipelines de qualidade baseados em quarentena** que preservam registros inválidos para auditoria e remediação.

## Objetivos Finais

- Construir **tabelas Bronze de streaming multi-flow** consolidando dados CSV e JSON de múltiplas fontes usando definições explícitas de **`CREATE FLOW`**.
- Criar **Materialized Views Gold incrementais** e aplicar **tags do Unity Catalog** em objetos Bronze, Silver e Gold para governança e descoberta.
- Implementar o **padrão Multiplex** para ingerir um fluxo de eventos de esquema misto em uma única tabela Bronze usando o tipo de dados **`VARIANT`**, distribuindo-os para tabelas Silver específicas de domínio.
- Construir um **Delta Sink** usando **`dp.create_sink`** e **`@dp.append_flow`**, habilitando leituras **Iceberg via Delta UniForm** para acesso analítico multiplataforma.
- Automatizar um **pipeline de CDC** usando **`AUTO CDC INTO`** com **SCD Tipo 2** para rastrear eventos de INSERT, UPDATE e DELETE de clientes com histórico completo, exibidos via Materialized Views Gold.
- Implementar **expectativas de qualidade de dados avançadas** abrangendo NOT NULL, intervalos numéricos e restrições tolerantes a NULL, além de lidar com **evolução de esquema** em tabelas Bronze usando **`schemaHints`** e **`_rescued_data`**.
- Aplicar o **Padrão de Quarentena** usando **lógica inversa** para rotear registros inválidos sem perda de dados e monitorar métricas de violação por restrição na **UI de Pipelines**.
