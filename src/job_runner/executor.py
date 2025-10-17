import asyncio as _asyncio
import concurrent.futures as _cf
import collections.abc as _cabc

class Executor:
    def __init__(self, loop: _asyncio.AbstractEventLoop, executor: _cf.Executor) -> None:
        self._loop = loop
        self._executor = executor

    async def run[*A, T](
        self,
        method: _cabc.Callable[[*A], T],
        *args: *A,
    ) -> T:
        return await self._loop.run_in_executor(self._executor, method, *args)