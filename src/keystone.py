import contextlib as _ctx
import pathlib as _pl
import typing as _tp

import keystoneauth1.identity.v3 as _kidv3
import keystoneauth1.session as _ksess
import yaml as _yaml


def create_application_credential() -> _kidv3.ApplicationCredential:
    clouds_file_path = (
        _pl.Path(__file__).parents[1] / "config" / "swiftoperator-clouds.yaml"
    )

    with clouds_file_path.open() as stream:
        data = _yaml.safe_load(stream)

    openstack = data["clouds"]["openstack"]
    auth = openstack["auth"]

    application_credential = _kidv3.ApplicationCredential(**auth)

    return application_credential


def _create_session() -> _ksess.Session:
    auth = create_application_credential()
    session = _ksess.Session(auth=auth)
    return session


@_ctx.contextmanager
def create_session() -> _tp.Generator[_ksess.Session]:
    session = _create_session()
    yield session
    session.invalidate()
