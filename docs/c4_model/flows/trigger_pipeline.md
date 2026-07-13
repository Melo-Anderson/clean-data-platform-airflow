# Nível 4: Fluxo - Trigger de Pipeline

Este diagrama de sequência descreve o fluxo detalhado quando um usuário solicita o disparo manual de um pipeline por meio da REST API da plataforma.

```mermaid
sequenceDiagram
    participant Client as 👤 Cliente HTTP / AE
    participant PipelineRouter as 🐍 PipelineRouter
    participant TriggerPipelineRunUseCase as ⚙️ TriggerPipelineRunUseCase
    participant SqlUnitOfWork as 🐘 SqlUnitOfWork
    participant DagGenerator as 📁 DagGenerator
    participant AirflowOrchestratorAdapter as ☁️ AirflowOrchestratorAdapter
    participant AirflowWebserver as ☁️ AirflowWebserver

    Client->>PipelineRouter: POST /api/v1/pipelines/{id}/run
    PipelineRouter->>TriggerPipelineRunUseCase: execute(pipeline_id, triggered_by)

    TriggerPipelineRunUseCase->>SqlUnitOfWork: find Pipeline by id
    SqlUnitOfWork-->>TriggerPipelineRunUseCase: Pipeline Object

    TriggerPipelineRunUseCase->>SqlUnitOfWork: save PipelineRun (status=RUNNING)

    TriggerPipelineRunUseCase->>DagGenerator: render Jinja2 template
    DagGenerator->>DagGenerator: Escreve arquivo físico da DAG (.py)<br>em /opt/airflow/dags/
    DagGenerator-->>TriggerPipelineRunUseCase: Arquivo salvo

    TriggerPipelineRunUseCase->>AirflowOrchestratorAdapter: trigger_dag(dag_id, run_id)

    loop Retry loop (até 10x com sleep de 5s)<br>para mitigar lag de parse do Scheduler
        AirflowOrchestratorAdapter->>AirflowWebserver: POST /api/v2/dags/{dag_id}/dagRuns

        alt 404: DAG ainda não identificada no banco do Airflow
            AirflowOrchestratorAdapter->>AirflowWebserver: POST /api/v2/dags/{dag_id}/refresh
            AirflowOrchestratorAdapter->>AirflowOrchestratorAdapter: sleep 5s
        else 201/200: Sucesso
            AirflowWebserver-->>AirflowOrchestratorAdapter: Retorna ID da execução (dag_run_id)
        end
    end

    AirflowOrchestratorAdapter-->>TriggerPipelineRunUseCase: dag_run_id
    TriggerPipelineRunUseCase->>SqlUnitOfWork: commit()
    TriggerPipelineRunUseCase-->>PipelineRouter: PipelineRun
    PipelineRouter-->>Client: 201 Created {run_id, status: "running", dag_run_id}
```

### Detalhamento do Processo

1. **Recepção**: O cliente envia uma requisição POST que passa primeiro pelo `AuthMiddleware`. O middleware verifica se o token JWT possui a permissão `pipeline:trigger`.
2. **Registro**: A entidade `PipelineRun` é gravada no banco com o status `RUNNING`.
3. **Escrita da DAG**: O `DagGenerator` renderiza um script contendo todos os operadores correspondentes ao pipeline. O arquivo é depositado no volume que o Airflow compartilha.
4. **Trigger Assíncrono**: O adaptador HTTP faz a chamada REST para disparar a execução. Caso o Airflow ainda não tenha indexado a nova DAG (o que gera 404), o adaptador envia uma chamada para a API `/refresh` e faz novas tentativas até o Scheduler identificar a DAG gerada no filesystem.
5. **Finalização**: O run_id gerado pelo Airflow é acoplado ao metadado da plataforma e a transação do banco de dados local é persistida (`commit`).
