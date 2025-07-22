import json as _json

import pytest as _pt

import jsonrpcserver as _jrpcs

# This module needs to be imported to define the JSON-RPC methods
import jrpcs_methods as _jrpcsm  # type: ignore


@_pt.mark.asyncio
async def test_set_loki_ip_address():
    request_data = {
        "id": 314152,
        "jsonrpc": "2.0",
        "method": "set_loki_ip_address",
        "params": {"loki_ip_address": "10.7.9.42"},
    }
    request_json = _json.dumps(request_data)

    dummy_context = object()

    response_data = await _jrpcs.async_dispatch_to_serializable(
        request_json, context=dummy_context
    )

    response_json = _json.dumps(response_data)
    print(response_json)

    assert isinstance(response_data, dict) and "error" not in response_data
