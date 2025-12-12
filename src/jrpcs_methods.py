import asyncio as _asyncio
import logging as _log

import jsonrpcserver as _jrpcs
import resultes_jsonrpc.jsonrpc.connection as _rjjc
import resultes_pydantic_models.runner as _mrunner

import context as _con
import job_runner.job_runner as _jr

_LOGGER = _log.getLogger(__name__)


# Make sure import of `jrpcm` is not "organized" away by VS Code
def configure() -> None:
    pass


@_rjjc.cancellable_async_validated_jrpcs_method(_mrunner.RunnerOptions)
async def set_options(
    context: _con.Context, value: _mrunner.RunnerOptions
) -> _jrpcs.Result:
    _LOGGER.info("Got setup options %s.", value)

    _LOGGER.info("Setting log level to %s.", value.log_level)
    root_logger = _log.getLogger()
    root_logger.setLevel(value.log_level)

    context.shall_remove_completed_jobs = value.shall_remove_completed_jobs

    return _jrpcs.Success()


@_rjjc.cancellable_async_validated_jrpcs_method(_mrunner.RunnerJob)
async def run_job(context: _con.Context, value: _mrunner.RunnerJob) -> _jrpcs.Result:
    _LOGGER.info("Running runner job %s.", value.id)

    loop = _asyncio.get_running_loop()

    job_runner = _jr.JobRunner(value, context, loop)

    connection = context.jsonrpc_connection

    try:
        async for payload in job_runner.run():
            notification = _mrunner.JobNotification(job_id=value.id, payload=payload)

            _LOGGER.debug("Sending notification %s for job %s.", notification)

            await connection.send_notification_base_model(
                "job_notification", notification
            )
    except Exception as exception:
        error_message = str(exception)

        _LOGGER.error("An error occurred running job %s: %s", value.id, error_message)

        notification = _mrunner.JobNotification.from_error(
            job_id=value.id, error_message=error_message
        )

        await connection.send_notification_base_model("job_notification", notification)

    return _jrpcs.Success()
