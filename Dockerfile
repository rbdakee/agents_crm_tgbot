FROM python:3.11-slim

# Install system dependencies (if needed for httpx/ssl)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency specification first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Ensure UTF-8 and no pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

# Expose port for webhook mode
EXPOSE 8080

# Default command runs the bot
CMD ["python", "main.py"]


