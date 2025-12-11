FROM python:3.11-slim
LABEL authors="Artur"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .

ENV API_HOST=0.0.0.0 \
    API_PORT=8082

CMD ["python", "-u", "__main__.py"]
