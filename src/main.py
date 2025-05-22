
import pydantic as _pyd
import tabella as _tab


class Vector3(_pyd.BaseModel):
    x: float
    y: float
    z: float


rpc = _tab.Tabella(title="ResulTES runner server", version="1.0.0")


@rpc.method()
async def create_variations(vector: Vector3) -> int:
    return 17


if __name__ == "__main__":
    rpc.run()
