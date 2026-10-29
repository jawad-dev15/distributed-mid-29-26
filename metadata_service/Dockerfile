FROM python:3.11-slim

WORKDIR /app

COPY metadata_service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY metadata_service/ metadata_service/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["python", "metadata_service/app.py"]
