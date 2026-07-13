# ADR 003: Modelo de Geração de DAGs via Filesystem Compartilhado e Jinja2 Templates

## Status
Aprovado

## Contexto
O orquestrador de tarefas (Apache Airflow) necessita de arquivos físicos escritos em Python declarando as tarefas (DAGs) para poder agendá-las e executá-las.
Por outro lado, a Platform API fornece aos usuários interfaces e rotas REST para criar e editar pipelines sob demanda. 
Precisamos de um mecanismo para sincronizar a criação lógica de pipelines na API com a infraestrutura de execução no Airflow.

## Decisão
Implementou-se a **geração automática e escrita física de DAGs Python em um volume de arquivos compartilhado**:
1. **Templates Dinâmicos**: A API utiliza geradores de arquivos Python (renderizados via **Jinja2** com a estrutura base de DAGs e o schema validado do pipeline).
2. **filesystem Compartilhado**: Os arquivos Python renderizados são salvos em um diretório comum (ex: `./dags`), montado tanto no container da `Platform API` quanto nos containers do `Airflow Scheduler` e `Webserver`.
3. **Trigger imediato**: A API dispara a execução da DAG comunicando-se com a API REST do Airflow imediatamente após salvar o arquivo físico, solicitando ao scheduler que recarregue a DAG (através de comandos de reserialize e refresh).

## Consequências
- **Positivas**:
  - Desacoplamento operacional: O Airflow gerencia a execução e paralelismo de forma independente, enquanto a API apenas define "o quê" rodar.
  - Visibilidade e rastreabilidade: DAGs geradas são arquivos Python legíveis por humanos e podem ser inspecionados para fins de auditoria e debug.
- **Negativas**:
  - Dependência de montagem e latência do sistema de arquivos compartilhado (volumes em nuvem/Docker) para sincronizar o arquivo no scheduler a tempo da execução imediata.
  - Concorrência de escrita de arquivos físicos concorrentes no mesmo diretório compartilhado.
