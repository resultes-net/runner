import keystone as _ks


def test_create_and_invalidate_session() -> None:
    with _ks.create_session():
        pass
