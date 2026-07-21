# E2E & Performance Testing Design for Discovery Engine

## 1. Contexto e Objetivos

O processo de *Metadata Discovery* precisa ser resiliente e escalável frente a ambientes físicos de dados que podem variar do cenário ideal (esquemas predefinidos) ao caos absoluto (NoSQL sem validações, bancos de dados legados massivos).

O objetivo deste design é estabelecer um padrão de testes **End-to-End (E2E)** focado em performance estrutural e cenários exóticos de autodescoberta, mantendo a performance da máquina local saudável durante o desenvolvimento.

## 2. Governança de Infraestrutura Local (Docker Compose Profiles)

Subir todo o ecossistema e múltiplos bancos simultaneamente degrada a experiência do desenvolvedor e o consumo da máquina local.

**Decisão Arquitetural:** Em vez de agrupar bancos em um único perfil, aplicaremos perfis extremamente granulares para os contêineres e injetaremos a carga a depender do foco de cada teste.

* `core`: Serviços fundamentais de infra (OpenBao, Postgres primário da plataforma).
* `airflow`: Serviços do orquestrador (Webserver, Scheduler).
* `api`: API principal da plataforma.
* **`e2e-mongo`**: Contêiner MongoDB isolado para testes de extração híbrida e NoSQL.
* **`e2e-pg-perf`**: Contêiner Postgres isolado para testes de estresse estrutural do Discovery.
* `e2e-runner`: O executor efêmero dos testes pytest.

## 3. Arquitetura de Seed Data (Test Fixtures Universais)

Para tornar os cenários de carga previsíveis, reutilizáveis e fáceis de versionar no projeto, abandonaremos a inicialização de tabelas e coleções via código Python no corpo dos testes e as centralizaremos.

* **Diretório:** Criaremos a pasta `scripts/e2e_seeds/`.
* **Conteúdo:**
  * `scripts/e2e_seeds/postgres_perf_schema.sql`
  * `scripts/e2e_seeds/mongo_init_schema.js`
* Esses arquivos serão montados (volume) nos respectivos contêineres e rodarão automaticamente no boot inicial (ex: `/docker-entrypoint-initdb.d/`).

## 4. Teste de Autodescoberta no MongoDB (E2E Híbrido)

Validar a resiliência no mapeamento de bancos orientados a documento.
A semente `mongo_init_schema.js` provisionará o banco `test_db` contendo:
1. **Coleção Estrita (`users_strict`):** Com validador formal do MongoDB usando `$jsonSchema` (representando o caso ideal).
2. **Coleção Dinâmica (`logs_loose`):** Sem regras definidas no banco, mas populada com um mix de documentos que a plataforma precisará analisar por amostragem probabilística (`$sample`).

O E2E testará se a plataforma mapeia ambos perfeitamente para o DTO `DataObject`.

## 5. Teste de Autodescoberta no Postgres (Performance Estrutural)

Garantir que a plataforma aguenta catálogos maciços sem estourar memória ou timeouts.
O arquivo `postgres_perf_schema.sql` usará PL/pgSQL dinâmico para forjar um ambiente de estresse tático:

* Geração de **300 tabelas sintéticas**.
* **Cobertura Holística:** A estrutura contemplará a presença de **todos os data types nativos do Postgres**, incluindo arrays, JSONB, geolocalização básica e tipos raros, concentrando os tipos exóticos em uma `edge_case_table` específica.
* **Complexidade de Metadados:** As tabelas terão dependências (Foreign Keys - FKs), Primary Keys (PKs), Views complexas associadas, além de comentários (descrições) em tabelas e colunas, e índices customizados.

### Métricas de Sucesso do Teste
1. O processo de Discovery mapeia integralmente as 300 tabelas + views, extraindo descrições, PKs, FKs e os respectivos tipos, sem omitir dados ou lançar exceções de conversão de metadados.
2. Nenhuma falha de OOM (Out Of Memory) no lado do Airflow ou da API, mantendo tempo de execução factível localmente.
