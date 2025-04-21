FROM ghcr.io/spf-ost/resultes-runner-prefect

SHELL ["powershell", "-command"]

RUN  New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force

WORKDIR /app

COPY requirements3.13/pytrnsys.txt requirements3.13/

RUN python -m pip install --no-cache-dir -r requirements3.13/pytrnsys.txt

WORKDIR /app/src

COPY src/worker.py src/workflow.py ./

ENTRYPOINT ["python", "worker.py"]
