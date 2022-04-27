FROM apache/airflow:2.2.5
ENV PYTHONUNBUFFERED 0
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt