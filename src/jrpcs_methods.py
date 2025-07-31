import logging as _log

import jsonrpcserver as _jrpcs
import pydantic as _pyd
import resultes_jsonrpc.jsonrpc.types as _rjjt
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import context as _con
import run_python_script_in_pytrnsys_venv as _rps


def dummy() -> None:
    pass


@_jrpcs.method()
async def run_python_script_in_pytrnsys_venv(
    context: _con.Context, runner_job: _rjjt.JsonStructured
) -> _jrpcs.Result:
    try:
        job = _mpytrnsys.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    _log.info("Running runner job %s.", job.id)

    return await _rps.run_python_script_in_pytrnsys_venv(context, job)
