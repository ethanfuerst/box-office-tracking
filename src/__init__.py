from pathlib import Path

project_root = Path(__file__).parent.parent
database_name = 'box_office_tracking_sqlmesh_db'
database_path = Path(
    project_root / 'src' / 'duckdb_databases' / f'{database_name}.duckdb'
)
