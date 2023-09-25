FROM python:3.10

WORKDIR /app

COPY main.py /app/
COPY data/ /app/data/

COPY requirements.txt /app/

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
