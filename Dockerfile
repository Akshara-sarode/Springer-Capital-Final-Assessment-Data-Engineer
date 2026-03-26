FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install pandas numpy

CMD ["python", "main.py"]
