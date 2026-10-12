# Guidance for agents working on `runner`

For the overall ResulTES architecture, dependency management and deployment, read the org-wide guide first:
https://github.com/resultes-net/issues/blob/main/AGENTS.md. This file only adds what's specific to this repo.

## Environment
- Runs on Windows only (OpenStack Windows VMs, TRNSYS in a subprocess), Python 3.13. A venv for development and tests has to
  be a Windows one.
- Entry point: `src/main.py`, a JSON-RPC (websockets) server on port 3000 that the scheduler connects to. The JSON-RPC
  methods are in `src/jrpcs_methods.py`, the job execution in `src/job_runner/`.
- The `systems` scripts (e.g. the post-processing in `systems/common/process/`) run with this repo's Python environment.
  Their third-party dependencies (e.g. `pvlib`) therefore go into `requirements-3.13/run-3rd-party.in`.

## Requirements
- `requirements-3.13/`: `run.txt` is the image's environment, `dev.txt` adds the development tools (Black, pytest, ...).
  Third-party packages are in the separate `*-3rd-party` files. They must be compiled on Windows (see the org-wide guide).
- The submodules are installed from their checkouts (`file:pydantic-models` in `run.txt`, editable in `dev.txt`). Moving a
  submodule pin therefore doesn't need recompiling the requirements, unless the submodule's own dependencies changed.

## Submodules and `pydantic-models`
- Git submodules: `pydantic-models`, `openstack-utils`, `jsonrpc`.
- The runner parses the scheduler's job requests with its *own* `pydantic-models` checkout
  (`resultes_pydantic_models.runner`). When the models change, move this repo's `pydantic-models` to the same commit as
  `server` and `scheduler`: an old checkout fails on new required fields and silently drops fields it doesn't know (e.g. a
  renamed parameter, which `systems` then can't find). The org-wide guide describes how to get an unpushed
  `pydantic-models` commit from another checkout.

## Deployment
- `.github/workflows/create-disk-image.yml` builds the disk image on each push to `main` (TRNSYS, Python, the sources and
  `run.txt`) and uploads it to OpenStack as `runner-disk-image`. Runners the scheduler creates afterwards use it, so
  pushing a `pydantic-models` pin here is part of the coordinated deployment of a model change.

## Tests
- `pytest.ini` sets `python_files = test_*.py`. There are no tests yet.
