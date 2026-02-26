FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching — only re-installs if requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL code
COPY . .

# The ETL runs forever — no CMD timeout
CMD ["python", "scheduler.py"]
