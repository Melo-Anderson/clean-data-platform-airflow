# Ativos de Dados de Negócio (Business DataAssets)

Este documento descreve as especificações e o modelo conceitual do DataAsset, a separação da entidade física Endpoint e o processo de Autodescoberta de metadados no cadastro da plataforma.

## 1. Entidade DataAsset
O **DataAsset** é a representação lógica de alto nível de um domínio de negócio no repositório de dados. O seu cadastro é durável e sofre pouca alteração ao longo do tempo.

### Atributos do DataAsset
- **ID / Nome**: Identificador único do ativo de dados (ex: `Vendas`, `Logs_Acesso`).
- **Descrição**: Detalhamento do domínio de negócio e sua finalidade.
- **Dono do Ativo (Asset Owner)**: Papel de negócio responsável (PO, PM ou Analytics Engineer).
- **Tags de Negócio**: Classificação temática (ex: `financeiro`, `marketing`, `core`).
- **PolicyTags (Segurança)**: Tags de classificação de sensibilidade (ex: `PII`, `Restrito`, `Público`) que serão herdadas por todos os objetos gerados a partir do ativo.
- **ID_Endpoint**: Referência ao Endpoint correspondente para acesso aos dados físicos.

---

## 2. Entidade Endpoint
O **Endpoint** isola as definições físicas, de conectividade e autenticação técnica de um ativo de dados. Eventuais mudanças de host, porta ou chaves não impactam o cadastro do DataAsset.

### Atributos do Endpoint
- **ID_Endpoint**: Identificador único da conexão.
- **Tipo de Conexão**: Categoria da fonte ou destino (`Database Oracle`, `REST API`, `SFTP Server`, `ETL Flow`, `Cloud Storage Bucket`).
- **Host / URL / Porta**: Endereço físico de acesso.
- **Referência de Credencial**: Referência segura (ex: nome do segredo ou chave no Vault/Secret Manager) onde as credenciais reais de autenticação estão armazenadas.

---

## 3. Autodescoberta de Metadados (Metadata Discovery)
Ao cadastrar um novo `DataAsset` (associando-o a um `Endpoint`), a plataforma executa de forma automática um gatilho de **Metadata Discovery** na origem dos dados para mapear a estrutura física:

- **Bancos de Dados**: Varredura para coletar nomes de tabelas, schemas de colunas, chaves primárias/estrangeiras e constraints.
- **Buckets de Arquivos**: Varredura para coletar nomes de arquivos estruturados, extensões, tipos de campos identificados, partições físicas e configurações de criptografia.

Estes dados coletados de forma automática alimentam o Banco de Metadados da Plataforma de Dados, servindo como base para futuras especificações conceituais e técnicas dos DataObjects.
