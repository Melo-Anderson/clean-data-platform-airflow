# Glossário da Plataforma de Dados

## DataAsset
Entidade conceitual de negócio estável que agrupa informações de governança (descrições, tags de segurança) e faz referência a um Endpoint de origem. É cadastrado por perfis de negócio (PO, PM, Analytics Engineer) e governa a linhagem e permissões do ativo.

## Endpoint
Representação técnica que armazena os dados físicos de conectividade (URLs, hosts, portas) e referências a credenciais de segurança em cofres externos.

## Metadata Discovery (Autodescoberta)
Mecanismo automático ativado no cadastro do DataAsset para varrer o Endpoint e inferir sua estrutura técnica (schemas, constraints, chaves, arquivos).

## DataObject
Representa uma tabela, arquivo, coleção ou entidade lógica sob a jurisdição de um DataAsset.

## DataElement
Atributo ou campo individual contido em um DataObject.

## Estados de Ciclo de Vida (Lifecycle States)
Estados governados de um DataAsset ao longo de sua existência: `Draft`, `Active`, `Deprecated` e `Archived`.