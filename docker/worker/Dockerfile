FROM ghcr.io/spf-ost/resultes-runner-prefect

SHELL ["powershell", "-command"]

RUN  New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force

WORKDIR /app

COPY requirements-3.13/pytrnsys.txt requirements/

RUN python -m uv pip install --no-cache-dir -r requirements/pytrnsys.txt

WORKDIR /app/src

COPY src/worker.py src/workflow.py ./

ENTRYPOINT ["python", "worker.py"]
