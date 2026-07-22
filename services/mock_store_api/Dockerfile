FROM python:3.12-slim
WORKDIR /app
COPY services/mock_store_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/ /app/services/
ENV PYTHONPATH=/app
CMD ["uvicorn", "services.mock_store_api.main:app", "--host", "0.0.0.0", "--port", "8081"]
