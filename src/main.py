import os as _os
import secrets as _secs
import socket as _soc
import random as _rand
import asyncio as _asyncio

import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import tabella as _tab

rpc = _tab.Tabella(title="ResulTES runner server", version="1.0.0")


@rpc.method()
async def create_variations(parameters: _pttes.TtesParameters) -> list[str]:
    seconds_to_sleep = _rand.uniform(0.0, 20.0)
    await _asyncio.sleep(seconds_to_sleep)
    return [_secs.token_hex(nbytes=6) for _ in range(4)]


if __name__ == "__main__":
    dev_host = f"{_soc.gethostname()}.local"
    host = _os.environ.get("HOST", dev_host)
    rpc.run(host=host, port=3000)
