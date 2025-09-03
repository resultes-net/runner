import logging as _log
import traceback as _tb

import jsonrpcserver as _jrpcs
import jsonrpcserver.codes as _jrpcsc
import pydantic as _pyd
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjjt
import resultes_pydantic_models.runner as _mrunner

import context as _con
import run_job as _rj

_LOGGER = _log.getLogger(__name__)


# Make sure import of `jrpcm` is not "organized" away by VS Code
def configure() -> None:
    pass


@_rjjs.cancellable_async_jrpcs_method
async def run_job(
    context: _con.Context, runner_job: _rjjt.JsonStructured
) -> _jrpcs.Result:
    try:
        job = _mrunner.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    _LOGGER.info("Running runner job %s.", job.id)

    try:
        return await _rj.run_job(context, job)
    except Exception as exception:
        _LOGGER.error("Exception occurred: %s", exc_info=exception)
        traceback = "\n".join(_tb.format_exception(exception))
        return _jrpcs.Error(_jrpcsc.ERROR_SERVER_ERROR, str(exception), traceback)
