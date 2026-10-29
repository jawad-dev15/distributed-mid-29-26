FROM python:3.11-slim

WORKDIR /app

COPY storage_node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY storage_node/app.py .

ENV PYTHONUNBUFFERED=1
ENV PORT=8001
ENV STORAGE_DIR=/data/chunks

VOLUME ["/data"]

EXPOSE 8001

CMD ["python", "app.py"]
