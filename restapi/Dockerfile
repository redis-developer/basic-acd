FROM python:3.10-slim
WORKDIR /app
COPY ./requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt
COPY ./restapi/log_conf.yaml ./src/main.py ./src/operations.py ./src/response.py ./src/states.py ./
COPY ./.env ./
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-config=log_conf.yaml"]