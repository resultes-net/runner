import run_python_script_in_pytrnsys_venv as _rps
import pydantic as _pyd
import resultes_pydantic_models.pytrnsys as _mpytrnsys
import server as _srv


import jsonrpcserver as _jrpcs
import loki_logger_handler.loki_logger_handler as _llh


import logging as _log


@_jrpcs.method()
async def set_loki_ip_address(_: _srv.Server, loki_ip_address: str) -> _jrpcs.Result:
    root_logger = _log.getLogger()

    existing_loki_log_handlers = [
        h for h in root_logger.handlers if isinstance(h, _llh.LokiLoggerHandler)
    ]
    if existing_loki_log_handlers:
        return _jrpcs.Error(
            -32000,
            "Loki IP address already set.",
            "The Loki IP address can only be set once.",
        )

    url = f"{loki_ip_address}:80/loki/api/v1/push"

    loki_log_handler = _llh.LokiLoggerHandler(
        url=url,
        additional_headers={"X-Scope-OrgID": "resultes"},
        labels={"service_name": "runner", "ip_address": loki_ip_address},
    )

    root_logger.addHandler(loki_log_handler)

    root_logger.info("Loki logging handler logging to IP address %s added.", loki_ip_address)

    return _jrpcs.Success()


@_jrpcs.method()
async def run_python_script_in_pytrnsys_venv(
    server: _srv.Server, runner_job: dict[str, _pyd.JsonValue]
) -> _jrpcs.Result:
    try:
        job = _mpytrnsys.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    return await _rps.run_python_script_in_pytrnsys_venv(server, job)