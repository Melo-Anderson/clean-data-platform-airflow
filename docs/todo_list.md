1. O CD não realiza um deploy real — crítica alta
O workflow declara um estágio de deploy, mas o job apenas exibe uma mensagem:

echo "Artifact compiled-dags is ready. Mock deploy completed successfully."

O próprio projeto reconhece que o deploy é educacional em ci_cd_pipeline.yml e em docs/ci_cd_guide.md.

Problema de engenharia: o pipeline comunica uma falsa sensação de Continuous Delivery. Ele valida e empacota artefatos, mas não comprova que uma versão foi publicada, promovida, disponibilizada ao Airflow ou validada em um ambiente real.

Recomendação:

separar explicitamente CI, build, publish e deploy;
publicar imagens em um registry;
gerar artefatos imutáveis associados ao SHA do commit;
implementar deploy real para um ambiente de staging;
adicionar smoke tests pós-deploy;
exigir aprovação manual para produção;
implementar rollback de DAGs, imagens e configurações.
2. Há credenciais inseguras por padrão no Docker Compose — crítica alta
O arquivo docker-compose.yml contém defaults como:

airflow/airflow para PostgreSQL;
root como token padrão do OpenBao;
admin/admin para Airflow;
test_secret_key para autenticação;
clean_data_platform_secret_key_fixed como chave JWT;
ALLOW_ORIGINS: '*'.
Embora sejam adequados para um ambiente local de demonstração, esses valores são perigosos porque podem ser reutilizados acidentalmente fora de desenvolvimento.

Problema de engenharia: segurança não deveria depender apenas da disciplina do operador ou de comentários na documentação.

Recomendação:

falhar na inicialização se secrets de produção não estiverem definidos;
remover defaults inseguros de autenticação;
separar arquivos compose.dev.yml, compose.test.yml e configuração de produção;
usar Docker secrets, OpenBao ou workload identity;
restringir CORS por ambiente;
impedir o uso de tokens de desenvolvimento em staging e produção;
adicionar testes automatizados que detectem credenciais default.
3. [RESOLVIDO] A configuração permite estados inválidos
> **Status:** Resolvido na versão atual. Adapters, algoritmos e estratégias foram tipados estritamente com `typing.Literal` em `app/config.py`, convertendo valores inválidos em falhas imediatas de inicialização (`ValidationError`). Testes unitários dedicados em `tests/unit/test_config.py`.

Em app/config.py, vários valores importantes eram strings livres:

Python
algorithm: str = "HS256"
secret_manager_adapter: str = "noop"
provisioner_adapter: str = "noop"
default_load_strategy: str = "full_load"
Isso permite que configurações inválidas sejam aceitas até ocorrer uma falha em runtime.

Problema de engenharia: uma plataforma robusta deve transformar erros de configuração em falhas rápidas e compreensíveis durante o startup ou no deploy.

Recomendação:

usar Enum ou tipos literais para adapters, algoritmos e estratégias;
validar URLs, tamanhos de pool, timeouts e caminhos;
definir configurações obrigatórias por ambiente;
criar um comando de validação, por exemplo:
bash
uv run python -m cli.main config validate --environment production
testar combinações inválidas e incompatíveis de configuração.
4. O escopo arquitetural está grande demais para o nível de maturidade operacional
O repositório combina:

FastAPI;
Airflow;
PostgreSQL;
OpenBao;
DuckDB;
dbt;
BigQuery;
MongoDB;
OpenTelemetry;
Prometheus;
geração de DAGs;
descoberta de schemas;
schema drift;
circuit breaker;
LangGraph/harness;
OmniBeam;
múltiplos adapters de compute.
Essa amplitude é impressionante, mas aumenta drasticamente a superfície de falha.

Problema de engenharia: complexidade arquitetural não é sinônimo de maturidade. Cada integração exige contratos, testes de compatibilidade, observabilidade, documentação operacional e ownership.

Risco: o time pode gastar mais esforço mantendo abstrações e adapters do que entregando capacidades confiáveis para os usuários.

Recomendação:

definir um “golden path” oficialmente suportado;
reduzir o número de integrações no núcleo;
marcar adapters como experimental, supported ou deprecated;
definir uma matriz de suporte por ambiente;
medir custo operacional por componente;
remover abstrações que não tenham pelo menos duas implementações reais ou uma necessidade clara de evolução.
5. [RESOLVIDO] As abstrações de Clean Architecture precisam ser comprovadas por regras arquiteturais
> **Status:** Resolvido na versão atual. Implementados testes estáticos baseados em AST em `tests/unit/architecture/test_clean_architecture_rules.py` que garantem que `app/domain` e `app/application` têm zero dependências externas de infraestrutura, frameworks ou ORMs.

A estrutura domain, application e infrastructure é bem organizada. Entretanto, a existência de diretórios e Protocols não garante isolamento real.

O projeto afirma que os casos de uso não devem depender diretamente de SQLAlchemy, mas não há evidência, no material analisado, de uma verificação automatizada abrangente dessas dependências.

Recomendação:

Adicionar testes arquiteturais que falhem quando:

app.domain importar FastAPI, Airflow ou SQLAlchemy;
app.application importar módulos de infraestrutura;
adapters acessarem diretamente routers;
regras de negócio forem implementadas em controllers;
dependências cruzarem camadas em direção proibida.
Ferramentas possíveis:

import-linter;
testes próprios sobre o grafo de imports;
regras de lint específicas;
validação no CI.
6. A cobertura mínima de 80% pode esconder áreas críticas sem teste
O CI exige:

bash
--cov=app --cov-fail-under=80
Isso é positivo, mas cobertura agregada não demonstra que os caminhos mais perigosos estão protegidos.

Possíveis lacunas que deveriam ter métricas próprias:

autorização RBAC;
rotação e expiração de tokens;
retries e circuit breakers;
idempotência de execução de pipelines;
migrations;
geração de DAGs;
schema drift;
falhas parciais no padrão Write-Audit-Publish;
reprocessamento;
concorrência;
perda de conexão com Airflow, OpenBao e BigQuery.
Recomendação:

estabelecer cobertura por pacote crítico;
medir cobertura de branches;
exigir testes negativos de autorização;
usar mutation testing no CI periodicamente;
definir testes de contrato para cada adapter externo;
publicar tendência de cobertura ao longo do tempo, não apenas um threshold.
7. O teste de migrations valida apenas um rollback limitado
O workflow executa:

bash
alembic upgrade head
alembic downgrade -1
alembic upgrade head
Isso testa apenas o downgrade da última migration.

Problema: migrations antigas podem estar quebradas, e o rollback completo pode não funcionar. Além disso, downgrade -1 não demonstra compatibilidade com dados reais ou com versões anteriores suportadas.

Recomendação:

testar upgrades a partir de versões históricas;
testar downgrade completo em ambiente descartável;
inserir dados representativos antes do rollback;
verificar invariantes e constraints após cada etapa;
definir política explícita: migrations são reversíveis ou forward-only;
testar compatibilidade entre versões da aplicação e do schema.
8. O teste E2E possui características frágeis e potencialmente inseguras
O serviço e2e-tests:

instala o Docker CLI durante a execução;
executa apt-get update;
acessa /var/run/docker.sock;
depende de serviços iniciados por Docker Compose;
aguarda disponibilidade usando loops com curl.
Trechos relevantes estão em docker-compose.yml.

Problemas:

o acesso ao Docker socket equivale, em muitos ambientes, a privilégios elevados sobre o host;
a instalação dinâmica aumenta variabilidade e tempo de execução;
o teste pode ficar verde apenas por disponibilidade superficial, e não por readiness real;
não há isolamento claro entre testes concorrentes;
o ambiente é difícil de reproduzir fora do GitHub Actions.
Recomendação:

criar uma imagem de teste versionada com Docker CLI já instalado;
evitar Docker socket sempre que possível;
utilizar containers efêmeros gerenciados por Testcontainers ou serviços nativos do CI;
implementar readiness probes reais;
coletar logs, métricas e artefatos de falha;
definir timeout por etapa;
executar E2E em uma pipeline separada e controlada.
9. Falta uma estratégia clara de supply chain security
O CI faz lint, type checking, testes e build, mas não há evidência de:

scan de vulnerabilidades em dependências;
scan de imagens Docker;
geração de SBOM;
assinatura de imagens;
verificação de provenance;
validação de secrets;
pinagem de actions por SHA;
política de dependências vulneráveis.
Além disso, várias dependências usam intervalos abertos, por exemplo:

TOML
"fastapi>=0.115"
"dbt-core>=1.8.0"
"google-cloud-bigquery>=3.20.0"
O uv.lock ajuda na reprodução, mas não substitui uma política de atualização e verificação da cadeia de fornecimento.

Recomendação:

usar Dependabot ou Renovate;
adicionar pip-audit ou equivalente;
usar Trivy, Grype ou Snyk para imagens;
gerar SBOM em cada build;
assinar imagens com Cosign;
usar SLSA/provenance;
fixar GitHub Actions por commit SHA;
adicionar secret scanning e dependency review.
10. O build da imagem não é validado com os mesmos critérios do runtime
O job build_image apenas executa:

bash
docker build -t data-platform:latest -f Dockerfile.api .
Ele não:

inicia a imagem;
executa health checks;
valida migrations dentro da imagem;
testa permissões de usuário;
verifica que o processo não roda como root;
escaneia vulnerabilidades;
confirma que a imagem contém apenas os artefatos necessários.
Recomendação:

Adicionar um teste de imagem:

bash
docker run -d --name platform-api-test ...
curl --fail http://localhost:8000/health/ready
docker inspect ...
Também seria importante:

usar usuário não privilegiado;
utilizar imagens base fixadas por digest;
aplicar multi-stage builds;
reduzir dependências instaladas em runtime;
verificar tamanho da imagem;
publicar apenas imagens imutáveis associadas ao commit.
11. A observabilidade está descrita, mas os SLOs não estão definidos
O projeto possui métricas, tracing e probes, segundo app/main.py e docs/operations_guide.md.

Entretanto, não aparecem claramente:

SLO de disponibilidade da API;
SLO de conclusão de pipelines;
limite aceitável de atraso de DAGs;
taxa de falha por adapter;
error budget;
alertas acionáveis;
runbooks associados aos alertas;
dashboards versionados;
correlação entre PipelineRun, logs, traces e execução no Airflow.
Problema: coletar métricas não é o mesmo que operar um serviço de forma confiável.

Recomendação:

Definir, por exemplo:

disponibilidade da API;
p95 de latência por endpoint;
taxa de sucesso de pipelines;
tempo máximo de descoberta;
atraso máximo de ingestão;
tempo de recuperação;
taxa de schema drift não tratado.
Cada alerta deve possuir severidade, owner e runbook.

12. [RESOLVIDO] A métrica HTTP pode criar cardinalidade excessiva
> **Status:** Resolvido na versão atual. O middleware de observabilidade extrai o template de rota parametrizado (`request.scope['route'].path`) via `_extract_matched_route`, registrando labels como `/api/v1/pipelines/{pipeline_id}/run` em vez de UUIDs concretos. Testes em `tests/unit/infrastructure/test_metrics_path_normalization.py`.

O projeto menciona labels como:

Text
method, path, status
Se path for registrado com valores dinâmicos — por exemplo, UUIDs ou IDs de pipeline — isso pode gerar cardinalidade excessiva no Prometheus.

Recomendação:

usar nomes de rota normalizados, como /v1/pipelines/{pipeline_id}/run;
nunca usar URL completa como label;
limitar labels a valores controlados;
monitorar número de séries;
testar comportamento sob alto volume.
13. [RESOLVIDO] Falta uma estratégia explícita de idempotência
> **Status:** Resolvido na versão atual. O endpoint `POST /v1/pipelines/{id}/run` aceita o header HTTP `Idempotency-Key`. O modelo `PipelineRunModel` possui índice único composto `(pipeline_id, idempotency_key)` e o caso de uso `TriggerPipelineRunUseCase` retorna a execução existente caso a chave coincida, evitando duplicação de execuções ou de disparos no Airflow. Testes em `tests/unit/application/test_trigger_pipeline_run_idempotency.py`.

A plataforma cria PipelineRun, gera DAGs e dispara execuções no Airflow. Esse fluxo pode ser repetido por:

retry do cliente;
timeout de rede;
retry automático;
reprocessamento;
operador clicando novamente;
falha após persistência, mas antes do disparo ao Airflow.
A documentação operacional descreve o fluxo, mas não deixa clara uma chave de idempotência ou garantia transacional entre o banco e o Airflow.

Risco: duplicidade de pipelines, DAG runs ou cargas no DWH.

Recomendação:

aceitar Idempotency-Key nos endpoints mutáveis;
criar constraints únicas;
usar outbox/event table;
registrar estado da interação com Airflow;
implementar reconciliação entre PipelineRun e DagRun;
documentar semântica de retry e exatamente-uma-vez versus pelo-menos-uma-vez.
14. [RESOLVIDO PARCIALMENTE] Geração dinâmica de DAGs aumenta o risco operacional
> **Status:** Validação sintática resolvida. Implementado o `DagSyntaxValidator` em `app/infrastructure/dag_generator/dag_validator.py`, que valida a árvore sintática (AST) do código Python gerado antes de gravar os arquivos no diretório `dags/`. Testes em `tests/unit/infrastructure/dag_generator/test_dag_syntax_validator.py` e `tests/integration/test_demo_dags_compilation.py`.

A pipeline gera arquivos Python de DAG a partir de templates e YAML. Isso cria uma dependência importante entre:

configuração;
versão do template;
código gerado;
versão do Airflow;
plugins;
ambiente de execução.
A geração é validada, mas o projeto deveria demonstrar maior controle sobre o artefato final.

Recomendação:

versionar o hash do template utilizado;
armazenar o YAML fonte junto ao artefato compilado;
gerar manifesto com DAGs, dependências e versão;
executar airflow dags list e validação de import no CI;
comparar o diff das DAGs antes do deploy;
ter rollback de artefatos;
evitar que uma geração parcial substitua todo o diretório de DAGs.
15. A dependência do filesystem compartilhado limita a escalabilidade
O Compose monta volumes como:

./dags;
./logs;
./data;
./dbt_project;
diretórios de saída do DuckDB e OmniBeam.
Esse modelo é conveniente localmente, mas não representa bem ambientes distribuídos ou Kubernetes.

Problemas:

concorrência entre workers;
consistência eventual;
limpeza de arquivos;
crescimento ilimitado de logs e dados;
dificuldade de recuperação;
acoplamento entre API, scheduler e workers.
Recomendação:

separar metadados de artefatos;
usar object storage para arquivos de entrada e saída;
usar IDs e manifests em vez de caminhos locais;
definir retenção e lifecycle policies;
testar execução em múltiplos workers;
documentar claramente quais dados são temporários e quais são persistentes.
16. Ausência de testes de carga e confiabilidade
O README afirma que o objetivo não é tuning em escala extrema, mas uma plataforma de dados ainda precisa conhecer seus limites.

Não há evidência suficiente de testes para:

múltiplos pipelines simultâneos;
grande volume de metadata discovery;
alta taxa de requests;
saturação de pool PostgreSQL;
execução concorrente de DuckDB;
geração simultânea de DAGs;
degradação de serviços externos.
Recomendação:

Criar testes de performance com metas explícitas:

throughput;
latência p95/p99;
número de pipelines concorrentes;
tamanho máximo de asset;
tempo de discovery;
comportamento sob falha de dependências;
recuperação após reinício.
Também seria útil adicionar testes de chaos/resilience para PostgreSQL, OpenBao, Airflow e BigQuery.

17. O CI não parece refletir integralmente a documentação
A documentação menciona branches main e develop, mas o workflow apresentado está configurado para main:

YAML
on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]
A documentação também descreve nomes de profiles e comandos que precisam permanecer rigorosamente sincronizados com o workflow real.

Problema: divergência entre documentação e automação gera falhas de onboarding e baixa confiança nos procedimentos.

Recomendação:

validar documentação automaticamente;
testar os comandos documentados em CI;
remover instruções obsoletas;
usar uma única fonte de verdade para comandos;
adicionar testes que confirmem que todos os links e paths citados existem.
18. Falta um modelo explícito de ownership e suporte
Há documentação de stakeholders, mas um sistema de produção precisa ir além de perfis conceituais.

Deveriam estar definidos:

owner por domínio;
owner por adapter;
responsável por incidentes;
SLA de correção;
política de depreciação;
canal de suporte;
processo de mudança;
matriz de escalonamento;
classificação de criticidade de cada pipeline.
Sem isso, a arquitetura pode estar bem documentada, mas a operação ainda dependerá de conhecimento tribal.
