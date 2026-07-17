# MongoDB Discovery & NoSQL Endpoint Design

## 1. Overview
The platform needs to support discovery and asset tracking for MongoDB databases. Unlike relational databases which provide strict schemas via `information_schema`, MongoDB is a schema-less NoSQL document database. This design extends the domain model to support NoSQL endpoints and introduces a hybrid schema inference strategy (Validation Rules fallback to Random Sampling) to accurately capture schema metadata without compromising performance.

## 2. Architecture & Domain Changes

### 2.1 Domain Model
- **`EndpointType`**: Add `NOSQL = "nosql"`.
- **`Endpoint`**: Create a new subclass `NoSqlEndpoint` in `app/domain/endpoints/endpoint.py`. This explicitly segregates NoSQL configurations from relational `DatabaseEndpoint` models, allowing frontend interfaces or future API contracts to evolve independently.
- **Router & Schemas**:
  - Add `NoSqlEndpointCreateRequest` in `app/infrastructure/http/schemas/endpoint_schemas.py`.
  - Add a new route `POST /nosql` in `endpoint_router.py`. This route will include the standard audit logging (via `BackgroundTasks` injected previously).

### 2.2 Infrastructure Dependency
- **Driver**: The official async MongoDB driver for Python, `motor`, will be added to `pyproject.toml` (`poetry add motor`). This is necessary to align with the platform's async-first architecture.

## 3. Discovery Runner Strategy

### 3.1 Runner Factory
- Modify `DiscoveryRunnerFactoryImpl` (`discovery_runner_factory.py`) to map instances of `NoSqlEndpoint` to a new `MongoDbRunner`.

### 3.2 MongoDbRunner Implementation
The `MongoDbRunner` will implement the `DiscoveryRunner` abstract interface. It connects to the cluster using the connection string resolved from the Vault.

**Execution Flow per Target Collection:**
1. **JSON Schema Validation (Primary Strategy):**
   - Execute `db.command("listCollections")` to retrieve the internal metadata for the specific collection.
   - Inspect the `options.validator.$jsonSchema` field.
   - If a strict JSON Schema validation rule is configured on the database level, parse the required and optional fields, mapping BSON types to the platform's normalized types (e.g., `STRING`, `INTEGER`, `BOOLEAN`).

2. **Random Sampling (Fallback Strategy):**
   - If no validation rules exist, execute an aggregation pipeline: `[{ "$sample": { "size": 100 } }]`.
   - Iterate over the returned sample documents in memory.
   - Dynamically build a union set of all discovered keys and infer their data types using Python's `type()` mapped to the platform's normalized schema.
   - If fields present varying types across documents, fallback to a `MIXED` or `STRING` generic normalized type.

3. **Metadata Capture:**
   - Retrieve row counts cheaply using `collection.estimated_document_count()`.
   - Package the discovered fields and metadata into `SchemaSnapshot` objects to be diffed and saved by the Discovery Orchestrator.

## 4. Considerations & Trade-offs
- **Sampling Size**: A sample size of 100 provides a balanced trade-off between performance (minimal I/O) and schema accuracy. Extremely sparse fields might be missed, but this is an accepted compromise in schema-less ecosystems.
- **Dependencies**: Adding `motor` adds binary dependencies (like `pymongo`), but it provides robust connection pooling and async event-loop integration, which is vastly superior to running a sync driver in a thread pool for I/O bound reflection loops.
- **Testing**: Requires mocking `AsyncIOMotorClient` heavily in unit tests to simulate `listCollections` payloads and `$sample` cursors. E2E tests for MongoDB are deferred unless a MongoDB container is added to the local Docker Compose stack.
