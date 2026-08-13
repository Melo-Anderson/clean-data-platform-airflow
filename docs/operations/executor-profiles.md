# Airflow Executor Profiles

## Dev (default — LocalExecutor)

Suitable for single-machine development and CI.

```sh
cp .env.example .env  # fill in values
docker compose --profile airflow up
```

Executor: `LocalExecutor` — tasks run in subprocesses on the scheduler.

## Production-Like Local (CeleryExecutor)

Use the override file to test Celery-based execution locally:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.override.yml --profile airflow-prod up
```

Executor: `CeleryExecutor` with Redis broker. Workers run in separate containers.

## Cloud / Kubernetes (KubernetesExecutor)

For GKE/EKS/AKS, use `KubernetesExecutor`. Each task spawns a K8s Pod.
See the [official Helm chart docs](https://airflow.apache.org/docs/helm-chart/).

## Key Differences

| Feature              | LocalExecutor | CeleryExecutor | KubernetesExecutor |
|----------------------|--------------|----------------|-------------------|
| Horizontal scaling   | No           | Yes            | Yes               |
| Separate workers     | No           | Yes            | Yes (per-task Pod)|
| CI / local dev       | Yes          | Possible       | No                |
| Production default   | No           | Common         | Cloud-native      |
