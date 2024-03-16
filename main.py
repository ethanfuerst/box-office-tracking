import modal
from etl.extract import extract
from etl.transform import transform
from etl.load import load

stub = modal.Stub("box-office-tracking")

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install duckdb==0.10.0",
    "pip install gspread-formatting==1.1.2",
    "pip install gspread==6.0.2",
    "pip install html5lib==1.1",
    "pip install lxml==5.1.0",
    "pip install pandas==2.1.4",
    "pip install requests==2.31.0",
    "pip install python-dotenv==1.0.1",
)


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 4 * * *"),
    secrets=[modal.Secret.from_name("box-office-tracking-secrets")],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[modal.Mount.from_local_dir("assets/", remote_path="/root/assets")],
)
def etl():
    extract()
    transform()
    load()


if __name__ == "__main__":
    # modal.runner.deploy_stub(stub)\
    etl.local()
