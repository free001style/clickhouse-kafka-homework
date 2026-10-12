FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY sql ./sql
COPY tests ./tests

ENTRYPOINT ["python", "-m", "app.pipeline"]
CMD ["--csv", "/data/train.csv", "--output-dir", "/results"]
