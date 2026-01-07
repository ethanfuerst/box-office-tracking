import logging

from sqlmesh.core.context import Context

from src import project_root
from src.utils.logging_config import setup_logging

setup_logging()


def main() -> None:
    logging.info('Running SQLMesh plan and apply.')
    sqlmesh_context = Context(paths=project_root / 'src' / 'sqlmesh_project')

    sqlmesh_context.plan(include_unmodified=True, auto_apply=True)
