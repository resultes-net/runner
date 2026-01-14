To get the RDP password for `runner` do the following.

1. Set the `OS_PASSWORD`:
    ```pwsh
    PS C:\Users\damian.birchler\src\resultes\runner\create-disk-image> $env:OS_PASSWORD=read-host
    ```

1. Get encrypted password:
    ```pwsh
    PS C:\Users\damian.birchler\src\resultes\runner\create-disk-image> .\scripts\nova.ps1 get-password runner > password.enc
    ```

1. Start WSL:
    ```bash
    PS C:\Users\damian.birchler\src\resultes\runner\create-disk-image> wsl
    ```

1. In WSL, decrypt:
    ```bash
    damian@hn602244:/mnt/c/Users/damian.birchler/src/resultes/runner/create-disk-image$ cat password.enc | base64 -d | openssl pkeyutl -decrypt -inkey config/runner_keypair.pem
    ```
