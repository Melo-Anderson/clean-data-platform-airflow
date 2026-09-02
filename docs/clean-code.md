# Guia de Boas Práticas e Padrões de Arquitetura em Python

Este documento estabelece as diretrizes normativas de **Clean Architecture**, **Domain-Driven Design (DDD)**, **Test-Driven Development (TDD)**, **Clean Code** e **The Twelve-Factor App** para projetos modernos em Python. Ele serve como padrão de referência agnóstico para qualquer aplicação corporativa ou plataforma que adote arquitetura em camadas.

---

## 1. Clean Architecture — Estrutura em Camadas

A arquitetura é estruturada em círculos concêntricos com a regra fundamental de dependência: **as dependências apontam exclusivamente de fora para dentro**.

```
┌───────────────────────────────────────────────────────────┐
│  Infrastructure Layer (app/infrastructure/)               │
│  ├── HTTP / Web Framework (FastAPI, Routers, Schemas)     │
│  ├── Persistence (SQLAlchemy, ORM Models, SQL Repos)     │
│  ├── External Adapters (Cloud SDKs, Gateways, Queues)     │
│  └── Configuration (Settings, Environment Resolvers)      │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Application Layer (app/application/)               │  │
│  │  ├── Use Cases / Command Handlers                   │  │
│  │  ├── Ports & Protocols (Interfaces de Saída)        │  │
│  │  └── DTOs & Service Contracts                       │  │
│  │                                                     │  │
│  │  ┌───────────────────────────────────────────────┐  │  │
│  │  │  Domain Layer (app/domain/)                   │  │  │
│  │  │  ├── Entities (Entidades de Negócio)          │  │  │
│  │  │  ├── Value Objects (Objetos de Valor)         │  │  │
│  │  │  ├── Domain Events & Exceptions               │  │  │
│  │  │  └── Domain Services & Builders               │  │  │
│  │  └───────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────┘
```

### Regras de Dependência e Isolamento de Camadas

| Camada | Pode depender de | Proibido depender de |
|---|---|---|
| **Domain** | Biblioteca padrão do Python (`stdlib`) | `application/`, `infrastructure/`, ORMs, frameworks web ou I/O externa |
| **Application** | `domain/`, Protocols/Interfaces declaradas | `infrastructure/` (implementações concretas de I/O) |
| **Infrastructure** | `application/`, `domain/`, bibliotecas externas e SDKs | — |

#### Diretrizes de Importação e PEP 8:
1. **Imports no Topo (Top-Level Imports):** Todos os imports devem residir no topo do arquivo. É estritamente proibido realizar imports inline no meio de funções para contornar dependências circulares.
2. **Uso de `TYPE_CHECKING`:** Para anotações de tipagem estática que gerariam dependência circular em runtime, utilize `if TYPE_CHECKING:` no topo do arquivo.
3. **Pureza Absoluta do Domínio:** Entidades e agregados do domínio nunca devem importar nem referenciar módulos de infraestrutura (como geradores de arquivos, serializadores de saída, bibliotecas de persistência ou clientes HTTP).

---

## 2. Domain-Driven Design (DDD)

### 2.1. Entidades vs. Value Objects

| Conceito | Características | Padrão no Código | Exemplo Conceitual |
|---|---|---|---|
| **Entidade** | Possui identidade única (`id`), mutabilidade de estado controlada por métodos de negócio | `@dataclass(kw_only=True)` | `Order`, `User`, `Account` |
| **Value Object** | Sem identidade própria, imutável por definição, igualdade por valor | `@dataclass(frozen=True)` | `Money`, `EmailAddress`, `DateRange` |

```python
# Exemplo de Value Object Imutável
@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: str = "BRL"

    def __post_init__(self) -> None:
        if self.amount < Decimal("0.00"):
            raise DomainValidationError("Amount cannot be negative")
```

### 2.2. Agregados e Invariantes de Negócio

- O **Agregado-Raiz (Aggregate Root)** é a única porta de entrada para mutação de estado de seu grafo de objetos.
- Entidades filhas e Value Objects nunca devem ser alterados diretamente por Use Cases; toda transição de estado deve ocorrer via métodos semânticos do Agregado que assegurem suas **invariantes de negócio**.

### 2.3. Fluent Builder Pattern para Agregados Complexos

Quando um agregado possui múltiplos Value Objects dependentes, configurações opcionais e regras de validação cruzadas, deve-se implementar um **Fluent Builder** no domínio:

```python
# app/domain/orders/order_builder.py
class OrderBuilder:
    def __init__(self, customer_id: str) -> None:
        self._customer_id = customer_id
        self._items: list[OrderItem] = []
        self._discount: Discount | None = None

    def with_item(self, product_id: str, quantity: int, unit_price: Decimal) -> Self:
        self._items.append(OrderItem(product_id=product_id, quantity=quantity, unit_price=unit_price))
        return self

    def with_discount(self, discount: Discount) -> Self:
        self._discount = discount
        return self

    def build(self) -> Order:
        if not self._items:
            raise DomainValidationError("An order must contain at least one item")
        return Order(
            customer_id=self._customer_id,
            items=tuple(self._items),
            discount=self._discount,
        )
```

---

## 3. The Twelve-Factor App & Gestão de Configurações

### 3.1. Modularização de Configurações (Separation of Concerns)

Configurações não devem ser condensadas em um modelo plano e monolítico. Devem ser agrupadas em sub-modelos coesos por responsabilidade:

```python
# app/config.py
class DatabaseSettings(BaseModel):
    url: str = "sqlite+aiosqlite:///:memory:"
    pool_size: int = 5
    max_overflow: int = 10

class AuthSettings(BaseModel):
    jwt_secret_key: str = ""
    jwt_algorithm: str = "RS256"
    jwt_public_key_pem: str = ""

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PLATFORM_",
        env_nested_delimiter="__",
        extra="allow",
    )

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
```

### 3.2. Eliminação de Constantes de Desenvolvimento e Hardcodings no Código Produtivo

1. **Código Agnóstico de Ambiente:** O código-fonte em produção (ex: Kubernetes, GKE, AWS ECS) não deve conter chaves RSA de teste, senhas de fallback ou constantes atreladas a containers locais.
2. **Injeção Externa de Segredos:**
   - **Em Produção:** Segredos, certificados e chaves devem ser injetados exclusivamente via Secret Manager (ex: Vault, AWS Secrets Manager, Kubernetes Secrets).
   - **Em Desenvolvimento Local:** Parâmetros de teste são fornecidos via arquivo `.env.dev` ou `docker-compose.yml`.
   - **Em Testes Automatizados:** Chaves e dados simulados são gerados dinamicamente via fixtures do `pytest` (`tests/conftest.py`).
3. **Falha Rápida (Fail-Fast):** Se uma credencial obrigatória estiver ausente na inicialização da aplicação em produção, a aplicação deve falhar imediatamente com erro explicativo (`ConfigurationError`), nunca adotar credenciais mockadas silenciosamente.

### 3.3. Centralização da Resolução de Variáveis e Fallbacks

- **Proibição de `os.environ.get()` nos Adaptadores:** Classes de infraestrutura e serviços nunca devem consultar variáveis de ambiente diretamente nem conter cadeias ternárias de fallback.
- **Single Source of Truth:** A resolução de precedência de caminhos ou arquivos de credenciais deve ser encapsulada em `@property` ou métodos do próprio módulo de configurações (`Settings`).

---

## 4. Test-Driven Development (TDD) e Pirâmide de Testes

### 4.1. Estrutura da Pirâmide

```
             ▲
            / \
           /E2E\         ← Testes de ponta a ponta contra ambiente completo
          /─────\
         / Integ \       ← Testes de integração com banco de dados real / containers
        /─────────\
       /   Unit    \     ← Testes unitários com Mocks e Fakes em memória
      /─────────────\
```

### 4.2. Regras e Boas Práticas para Testes

1. **Princípio F.I.R.S.T:**
   - **Fast:** Execução rápida (centenas de testes por segundo em unitários).
   - **Independent:** Cada teste deve ser executável isoladamente sem ordem pré-definida.
   - **Repeatable:** Resultados idênticos em qualquer máquina ou pipeline de CI.
   - **Self-Validating:** Resultado booleano claro (Pass/Fail) sem inspeção manual.
   - **Timely:** Escritos antes ou durante o desenvolvimento do código de produção (Red-Green-Refactor).
2. **Named Fakes e Mocks Explícitos:** Em vez de `MagicMock` anônimos com comportamento opaco, utilize classes de teste nomeadas que implementem os protocolos oficiais:
   ```python
   class InMemoryOrderRepository:
       def __init__(self) -> None:
           self._storage: dict[str, Order] = {}

       async def save(self, order: Order) -> None:
           self._storage[order.id] = order

       async def find_by_id(self, order_id: str) -> Order | None:
           return self._storage.get(order_id)
   ```
3. **Testes Baseados em Propriedades (Hypothesis):** Utilize geração automática de entradas para validar invariantes matemáticas, algoritmos de parsing e integridade de Value Objects.
4. **Testes de Caos e Resiliência (Fault Injection):** Simule falhas de rede, timeouts e respostas HTTP 500/503 em adaptadores externos para validar retries e abertura de Circuit Breakers.
5. **Testes de Mutação (Mutation Testing):** Avalie a efetividade dos testes unitários inserindo mutações de código em camadas de domínio e aplicação para certificar que os testes realmente falham quando a lógica é alterada.

---

## 5. Padrões de Qualidade de Código (Clean Code)

### 5.1. Tamanho e Coesão
- **Funções:** 4 a 20 linhas. Cada função deve fazer apenas uma coisa e fazê-la bem (**Single Responsibility Principle**).
- **Módulos / Arquivos:** Máximo de 300 linhas. Ultrapassando esse limite, decomponha em submódulos coesos.

### 5.2. Nomenclatura Semântica
- Use nomes substantivos para classes e entidades (`OrderProcessor`, `NotificationDispatcher`).
- Use nomes verbais para métodos e funções (`calculate_total`, `publish_event`).
- Evite sufixos genéricos vazios como `Helper`, `Utils`, `Common`, `Manager` ou `Data`.

### 5.3. Tipagem Estática Rigorosa (Python Moderno)
- Tipagem estática de 100% dos parâmetros e retornos de funções.
- Utilize a sintaxe moderna de tipos (PEP 585 e PEP 604): `dict[str, Any]`, `list[str]`, `str | None` (evite `Optional`, `Union`, `Dict`, `List` da biblioteca `typing`).
- Proibido o uso de `Any` sem justificativa técnica explícita.

### 5.4. Tratamento de Erros e Padrão RFC 7807

- Toda API HTTP deve responder a erros no formato padronizado **RFC 7807** (`application/problem+json`).
- **Nunca lance exceções HTTP (como `HTTPException`) dentro de Use Cases ou no Domínio.**
- Defina uma hierarquia clara de exceções de domínio mapeadas globalmente em manipuladores de erro na camada HTTP:

```python
# app/domain/shared/exceptions.py
class DomainException(Exception):
    """Exceção base de regras de negócio."""

class EntityNotFoundError(DomainException):
    """Recurso não encontrado (Mapeado para HTTP 404)."""

class DomainValidationError(DomainException):
    """Violação de invariante de negócio (Mapeado para HTTP 422)."""

class UnauthorizedAccessError(DomainException):
    """Credenciais inválidas ou ausentes (Mapeado para HTTP 401)."""

class ForbiddenActionError(DomainException):
    """Ação não permitida para o perfil (Mapeado para HTTP 403)."""

class InfrastructureFailureError(Exception):
    """Falha não recuperável de serviço externo (Mapeado para HTTP 500/503)."""
```

---

## 6. Padrões de Infraestrutura e Resiliência

### 6.1. Strategy / Registry Pattern para Adaptadores Plugáveis (Open/Closed Principle)

Fábricas de adaptadores intercambiáveis (ex: provedores de pagamento, motores de processamento, drivers de banco) não devem usar estruturas estáticas de `if/elif/else`. Devem utilizar um **Registry** desacoplado:

```python
# app/infrastructure/adapters/registry.py
class AdapterRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, Callable[..., Any]] = {}

    def register(self, key: str, factory: Callable[..., Any]) -> None:
        self._factories[key.lower()] = factory

    def get(self, key: str) -> Callable[..., Any]:
        factory = self._factories.get(key.lower())
        if not factory:
            raise UnsupportedAdapterError(f"No adapter registered for key: '{key}'")
        return factory
```
*Isso permite registrar novos adaptadores em runtime ou em plugins sem alterar o código existente.*

### 6.2. Unit of Work (UoW) e Ciclo de Vida Transacional

- Use cases recebem um `UnitOfWork` por injeção de dependência.
- O `UoW` é a única entidade autorizada a realizar `commit()` e `rollback()`.
- O acesso aos repositórios pertencentes ao UoW deve ser restrito ao bloco de contexto assíncrono:
  ```python
  async with uow:
      user = await uow.users.find_by_id(user_id)
      user.update_profile(...)
      await uow.users.save(user)
      await uow.commit()
  ```

### 6.3. Ambientes e Redes Agnósticos (Zero Hardcoded Networking)

- É proibido embutir heurísticas de reescrita mágica de strings de conexão (ex: `if in_docker host = "postgres" else host = "localhost"`).
- O endereço do host fornecido pelo mecanismo de configuração ou Service Discovery deve ser o endereço exato e final consumido pelo cliente de conexão.
- Ambientes distintos (Kubernetes, AWS VPC, Docker local) devem gerenciar o roteamento via DNS, Service Discovery ou variáveis de ambiente dedicadas.

### 6.4. Estratégia de Caching em Adaptadores de I/O Remota

Para adaptadores que realizam consultas remotas repetitivas a serviços de nuvem ou catálogos externos, implemente cache em memória com TTL (Time-To-Live) baseado em tempo monotônico (`time.monotonic()`), com expiração configurável via `Settings`.

### 6.5. Resiliência com Circuit Breaker e Retries

- Chamadas a APIs e serviços externos devem ser protegidas por disjuntores de circuito (**Circuit Breakers**) e políticas de retentativa com backoff exponencial (**Exponential Backoff**).
- Quando o serviço externo falhar repetidamente além do limiar permitido, o circuito deve abrir imediatamente para evitar esgotamento de threads e conexões.

---

## 7. Polimorfismo e Contratos do Sistema (Ports & Adapters)

Para garantir que a aplicação seja 100% agnóstica a bancos de dados, provedores de nuvem, mensageria e frameworks, todas as integrações com o mundo externo utilizam **Polimorfismo de Interface** via `Protocol` Python na camada de Aplicação.

```
┌────────────────────────────────────────────────────────────┐
│                      Application Layer                     │
│                                                            │
│   ┌────────────────────────────────────────────────────┐   │
│   │           <<Protocol>> NotificationPort            │   │
│   │   + send_notification(recipient, message): None    │   │
│   └────────────────────────────────────────────────────┘   │
└──────────────────────────────▲─────────────────────────────┘
                               │ implements
┌──────────────────────────────┴─────────────────────────────┐
│                    Infrastructure Layer                    │
│  ┌───────────────────────┐       ┌──────────────────────┐  │
│  │   SmtpEmailAdapter    │       │     SmsTwilioAdapter │  │
│  └───────────────────────┘       └──────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

### Regras de Ouro dos Contratos:
1. **Definição na Camada Interna:** As portas (`Ports`) são definidas na camada de `Application` (ou `Domain`). A `Infrastructure` apenas as implementa.
2. **Injeção de Dependência:** O Use Case recebe a porta abstrata em seu construtor, nunca a implementação concreta de infraestrutura.
3. **Facilidade de Teste:** Toda porta deve poder ser substituída nos testes por uma implementação in-memory sem uso de rede ou dependências externas.
