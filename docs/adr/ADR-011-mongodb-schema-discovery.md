# ADR 011: Estratégia de Schema Discovery Híbrido para MongoDB (Validator vs Sampling)

## Status
Aprovado

## Contexto
Diferente dos bancos de dados relacionais (SQL), que possuem schemas rígidos descritos nos catálogos de metadados do sistema, o MongoDB é orientado a documentos e inerentemente *schemaless* (ou *schema-flexible*). Coleções podem conter documentos com estruturas totalmente heterogêneas.

Para que a nossa plataforma de dados consiga mapear a linhagem de dados, desvios de schema (drifts) e aplicar tags de políticas de governança, precisamos descobrir a estrutura física de coleções do MongoDB de forma confiável e eficiente. Além disso, precisamos dar flexibilidade ao usuário para ignorar tabelas ou coleções temporárias/de sistema que não necessitam de governança ativa, exigindo a inclusão de um mecanismo de exclusão de escopo (`scope_exclude`).

## Decisão
Adotamos uma abordagem híbrida de extração de metadados no `MongoDbRunner` integrada ao nosso protocolo de discovery:

1. **Validação do Schema Formal (`$jsonSchema` - Rápido/Fiel)**:
   - O runner tenta ler as opções da coleção no banco de dados buscando um validador `$jsonSchema` formalmente cadastrado.
   - Caso exista, mapeamos suas propriedades diretamente para nossos tipos primitivos padronizados (`SchemaField`). Esta operação é extremamente barata (complexidade $O(1)$) e 100% precisa em relação às regras de validação do banco.

2. **Amostragem Dinâmica (`$sample` - Fallback/Inferência)**:
   - Se a coleção não possuir um validador cadastrado, executamos uma query de agregação utilizando o operador `$sample` para extrair aleatoriamente `100` documentos.
   - Analisamos dinamicamente as chaves e os tipos de dados contidos nessa amostra para fundir e inferir um schema representativo unificado. Chaves com tipos conflitantes na amostra são generalizadas como strings de forma segura.

3. **Mecanismo de Exclusão de Escopo (`scope_exclude`)**:
   - Estendemos a assinatura do contrato `DiscoveryRunner` e suas implementações para suportar glob-patterns de exclusão de escopo. As coleções que corresponderem aos padrões especificados em `scope_exclude` são ignoradas imediatamente durante a varredura inicial.

## Consequências
- **Positivas**:
  - **Eficiência**: Baixíssimo impacto computacional no banco para coleções governadas por schemas formais.
  - **Resiliência e Cobertura**: Capacidade de prover visibilidade e governança mesmo sobre coleções completamente dinâmicas ou desestruturadas.
  - **Menos Ruído**: A exclusão de escopo evita o provisionamento desnecessário de objetos de metadados indesejados (como coleções temporárias, caches ou logs).
- **Negativas**:
  - **Acurácia Estatística**: A amostragem de fallback (`$sample` de 100) pode não capturar 100% dos campos de documentos raros ou de coleções altamente heterogêneas.
  - **Custo Computacional Adicional**: Executar o operador `$sample` em coleções massivas sem índices apropriados pode causar impacto temporário de I/O no banco, embora mitigado pelo tamanho baixo da amostra (100).
