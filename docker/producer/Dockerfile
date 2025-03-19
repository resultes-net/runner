FROM ghcr.io/spf-ost/resultes-runner-prefect

SHELL ["powershell", "-command"]

WORKDIR /app/src

COPY src/producer.py src/workflow.py ./

ENTRYPOINT ["python", "producer.py"]
