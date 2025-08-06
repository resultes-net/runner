import logging as _log

import jsonrpcserver as _jrpcs
import pydantic as _pyd
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjjt
import resultes_pydantic_models.runner as _mrunner

import context as _con
import run_job as _rj


# Make sure import of `jrpcm` is not "organized" away by VS Code
def configure() -> None:
    pass


@_rjjs.cancellable_jrpcs_method
async def run_job(
    context: _con.Context, runner_job: _rjjt.JsonStructured
) -> _jrpcs.Result:
    try:
        job = _mrunner.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    _log.info("Running runner job %s.", job.id)

    return await _rj.run_job(context, job)
