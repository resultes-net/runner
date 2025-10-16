import asyncio as _asyncio
import logging as _log

import jsonrpcserver as _jrpcs
import pydantic as _pyd
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjjt
import resultes_pydantic_models.runner as _mrunner

import context as _con
import job_runner as _jr

_LOGGER = _log.getLogger(__name__)


# Make sure import of `jrpcm` is not "organized" away by VS Code
def configure() -> None:
    pass


@_rjjs.cancellable_async_jrpcs_method
async def set_options(
    context: _con.Context, runner_options: _rjjt.JsonStructured
) -> _jrpcs.Result:
    try:
        options = _mrunner.RunnerOptions(**runner_options)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    _LOGGER.info("Got setup options %s.", options)

    _LOGGER.info("Setting log level to %s.", options.log_level)
    root_logger = _log.getLogger()
    root_logger.setLevel(options.log_level)

    context.shall_remove_completed_jobs = options.shall_remove_completed_jobs

    return _jrpcs.Success()


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

    loop = _asyncio.get_running_loop()

    job_runner = _jr.JobRunner(job, context, loop)

    return await job_runner.run()
