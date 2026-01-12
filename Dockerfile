# Robo Analyzer Backend Dockerfile
# FastAPI + LangChain

FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (including build tools for numpy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python dependencies first
RUN pip install --no-cache-dir -r requirements.txt

# Reinstall numpy 1.x for legacy CPU compatibility (numpy 2.x requires X86_V2)
RUN pip uninstall -y numpy && pip install --no-cache-dir "numpy<2"

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p /app/logs

# Expose port
EXPOSE 5502

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5502/health || exit 1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5502"]
