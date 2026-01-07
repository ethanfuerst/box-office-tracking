def extract(*args, **kwargs):
    from src.etl.extract.main import main as _main

    return _main(*args, **kwargs)


__all__ = ['extract']
