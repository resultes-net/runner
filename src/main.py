import asyncio as _asyncio
import collections.abc as _cabc

import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import resultes_pydantic_models.simulations.variation as _var

import tabella as _tab


rpc = _tab.Tabella(title="ResulTES runner server", version="1.0.0")


@rpc.method()
async def create_variations(vector: _sb.Vector3) -> _cabc.Sequence[_var.Variation]:
    return _var.Variation()


if __name__ == "__main__":
    rpc.run()
