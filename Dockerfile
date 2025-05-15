FROM python:3.10-slim
LABEL authors="Magudapathi A"

WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y gcc g++ libeccodes0 libeccodes-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install -r requirements.txt

#COPY entrypoint.sh .
COPY . .

#ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
