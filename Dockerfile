# Dashboard Streamlit (equivalente a: streamlit run dashboard.py).
# En Render: Web Service con Dockerfile path = Dockerfile (root).
# Secrets/env: DATABASE_URL, MAPS_SOURCE, GOOGLE_MAPS_API_KEY, DASHBOARD_MODE, etc.
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render inyecta PORT; Streamlit debe escuchar en 0.0.0.0
ENV PORT=8501
EXPOSE 8501

CMD ["sh", "-c", "streamlit run dashboard.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false"]
