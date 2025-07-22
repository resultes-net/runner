import pytest as _pt
import logging as _log
import time as _time

import loki_logging as _llog
import log_config as _clog


@_pt.mark.parametrize("loki_ip_address", ["127.0.0.1"])
@_pt.mark.asyncio
async def test_loki_logging(loki_ip_address: str) -> None:
    _log.basicConfig(format=_clog.LOG_FORMAT, level=_log.INFO)

    root_logger = _log.getLogger()

    _llog.add_loki_log_handler(loki_ip_address, root_logger)

    for _ in range(30):
        _log.info("This is a test log to Loki. The answer is - as always - %d.", 42)
        seconds = 1.0
        _time.sleep(seconds)
