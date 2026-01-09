def extract(*args, **kwargs):
    from src.etl.extract.main import main as _main

    return _main(*args, **kwargs)


def transform(*args, **kwargs):
    from src.etl.transform.main import main as _main

    return _main(*args, **kwargs)


def load(*args, **kwargs):
    from src.etl.load.main import main as _main

    return _main(*args, **kwargs)


__all__ = ['extract', 'transform', 'load']
