import logging as _log

import jsonrpcserver as _jrpcs
import pydantic as _pyd
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import context as _con
import jsonrpc_logging as _llog
import run_python_script_in_pytrnsys_venv as _rps


@_jrpcs.method()
async def run_python_script_in_pytrnsys_venv(
    server: _con.Context, runner_job: dict[str, _pyd.JsonValue]
) -> _jrpcs.Result:
    try:
        job = _mpytrnsys.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    return await _rps.run_python_script_in_pytrnsys_venv(server, job)
