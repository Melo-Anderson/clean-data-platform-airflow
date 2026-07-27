FROM python:3.12-slim
WORKDIR /app
RUN pip install uv --no-cache-dir
COPY pyproject.toml ./
RUN uv sync --no-dev
COPY src/ ./src/
RUN adduser --disabled-password --gecos "" harness
USER harness
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "src.infrastructure.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
