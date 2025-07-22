import logging as _log

import loki_logger_handler.loki_logger_handler as _llh
import loki_logger_handler.formatters.logger_formatter as _lformat


def add_loki_log_handler(loki_ip_address: str, root_logger: _log.Logger) -> None:
    url = f"http://{loki_ip_address}:80/loki/api/v1/push"

    loki_log_handler = _llh.LokiLoggerHandler(
        url=url,
        labels={
            "service_name": "runner",
            "ip_address": loki_ip_address,
        },
        label_keys=["taskName"],
        additional_headers={"X-Scope-OrgID": "resultes"},
        enable_self_errors=True,
    )

    root_logger.addHandler(loki_log_handler)
