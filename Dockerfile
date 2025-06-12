FROM python:3.10-slim
LABEL authors="Magudapathi A"

WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system packages first
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libeccodes0 \
    libeccodes-dev \
    libmariadb-dev \
    pkg-config \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install wheel
RUN pip install --upgrade pip wheel

RUN pip install pygrib

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app code
COPY . .
