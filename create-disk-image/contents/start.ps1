New-NetFirewallRule -DisplayName ResulTES -Direction Inbound `
    -Program E:\runner\python\python.exe `
    -Profile Public -Protocol TCP -LocalPort 3000 -Action Allow

Add-MpPreference -ExclusionPath "C:\resultes\jobs"

Set-Alias -Name nssm -Value E:\nssm-2.24\nssm-2.24\win64\nssm.exe

nssm install ResulTES E:\runner\python\python.exe E:\runner\src\main.py

nssm set ResulTES ObjectName NetworkService

nssm set ResulTES AppStdout C:\resultes\logs\service.log
nssm set ResulTES AppStderr C:\resultes\logs\service-error.log

nssm set ResulTES AppEnvironmentExtra `
    JOBS_DIR_PATH=C:\resultes\jobs `
    LOG_FILE_PATH=C:\resultes\logs\runner.log

nssm start ResulTES
