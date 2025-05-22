import collections.abc as _cabc
import secrets as _secs

import openrpc as _orpc
import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import tabella as _tab

rpc = _tab.Tabella(
    title="ResulTES runner server",
    version="1.0.0",
    servers=[_orpc.Server(name="HTTP API", url="http://localhost:3000")],
)


@rpc.method()
async def create_variations(parameters: _pttes.TtesParameters) -> _cabc.Sequence[str]:
    return [_secs.token_hex(nbytes=6) for _ in range(4)]


if __name__ == "__main__":
    rpc.run()
