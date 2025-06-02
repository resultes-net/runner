import contextlib as _ctx
import typing as _tp

import keystoneauth1.identity.v3 as _kidv3
import keystoneauth1.session as _ksess

import clouds_yaml as _cyaml


def create_application_credential() -> _kidv3.ApplicationCredential:
    openstack = _cyaml.get_clouds_yaml_openstack_json()

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
