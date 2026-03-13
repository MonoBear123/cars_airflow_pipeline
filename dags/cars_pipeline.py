import json
import logging
import polars as pl

from sklearn.preprocessing import OneHotEncoder
from pathlib import Path
from datetime import datetime
from airflow.sdk import dag, task
from hooks import CarsHook

DATA_PATH = "/data/custom_hook/cars.json"
CLEAN_PATH = "/data/custom_hook/cars_clean.json"
logger = logging.getLogger(__name__)


@dag(
    dag_id="cars_pipline",
    description="Fetches car data from the custom API using a custom hook.",
    start_date=datetime(2026, 2, 3),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
)
def cars_analyz():

    @task
    def _fetch_cars(conn_id: str, output_path: str, batch_size: int = 1000) -> None:
        output = Path(output_path)

        logger.info("Fetching all cars from the API...")
        hook = CarsHook(conn_id=conn_id)
        cars = list(hook.get_cars(batch_size=batch_size))
        logger.info(f"Fetched {len(cars)} car records")

        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            json.dump(cars, f)

        logger.info(f"Saved cars to {output}")

    @task
    def _clean_cars_data(input_path: str, output_path: str) -> None:
        logger.info("Start preprocessing")

        with open(input_path, "r") as f:
            cars = json.load(f)

        df = pl.DataFrame(cars)

        logger.info(f"Initial shape: {df.shape}")
        df = df.unique()
        logger.info(f"After dedup: {df.shape}")

        categorical_cols = df.select(pl.col("Fuel_type", "Transmission")).to_pandas()
        logger.info(f"Categorical:\n{categorical_cols.head()}")

        encoder = OneHotEncoder(
            sparse_output=False, drop="first", handle_unknown="ignore"
        )
        encoded = encoder.fit_transform(categorical_cols)
        feature_names = encoder.get_feature_names_out()
        logger.info(f"New features: {list(feature_names)}")

        encoded_df = pl.DataFrame(encoded, schema=list(feature_names))

        df = df.drop("Fuel_type", "Transmission")
        df = df.hstack(encoded_df)

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        df.write_json(output)

        logger.info(f"Saved cleaned data to {output}")

    fetch = _fetch_cars("carsapi", DATA_PATH)
    clean = _clean_cars_data(DATA_PATH, CLEAN_PATH)
    fetch >> clean


cars_analyz()
