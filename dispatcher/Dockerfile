FROM python:3.10-slim
WORKDIR /app
COPY ./requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt
COPY ./src/dispatcher.py ./src/operations.py ./src/response.py ./src/states.py ./
COPY ./.env ./
CMD ["python3", "dispatcher.py"]