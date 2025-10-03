FROM python:3.11-slim

# Install system dependencies and security updates
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        chromium \
        chromium-driver \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*

# Create app user for security
RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

# Copy dependency specification first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create directories and set permissions
RUN mkdir -p /app/logs /app/data && \
    chown -R app:app /app

# Switch to app user
USER app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    CHROME_BIN=/usr/bin/chromium \
    CHROME_PATH=/usr/bin/chromium

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8081/health || exit 1

# Expose ports
EXPOSE 8080 8081

# Default command runs the bot
CMD ["python", "main.py"]


