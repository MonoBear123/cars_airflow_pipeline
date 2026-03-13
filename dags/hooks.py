import requests
from airflow.sdk.bases.hook import BaseHook
from typing import Any, Dict, Generator


class CarsHook(BaseHook):
    """
    Hook for the Car Data API.

    Abstracts authentication, pagination, and connection details for the /cars endpoint.
    """

    DEFAULT_SCHEMA = "http"
    DEFAULT_PORT = 8081

    def __init__(self, conn_id: str, retry: int = 3):
        super().__init__()
        self._conn_id = conn_id
        self._retry = retry
        self._session = None
        self._base_url = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_conn(self):
        if self._session is None:
            config = self.get_connection(self._conn_id)

            if not config.host:
                raise ValueError(f"No host specified in connection '{self._conn_id}'")

            schema = config.schema or self.DEFAULT_SCHEMA
            port = config.port or self.DEFAULT_PORT
            self._base_url = f"{schema}://{config.host}:{port}"

            self._session = requests.Session()
            if config.login:
                self._session.auth = (config.login, config.password)

        return self._session, self._base_url

    def close(self):
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None

    def get_cars(self, batch_size: int = 100) -> Generator[Dict[str, Any], None, None]:
        """Fetches all cars from the /cars endpoint with pagination."""
        session, base_url = self.get_conn()
        url = f"{base_url}/cars"

        offset = 0
        total = None

        while total is None or offset < total:
            params = {"offset": offset, "limit": batch_size}
            response = session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            yield from data["result"]

            offset += batch_size
            total = data["total"]

            if len(data["result"]) == 0:
                break
