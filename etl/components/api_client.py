from typing import Dict, Any, List
import logging
import requests
from .logger_configs import setup_logging

setup_logging()


class APIClient:
    """
    Класс для получения данных по API.
    """

    def __init__(self, url: str):
        self._url = url
        self._logger = logging.getLogger("APIClient")

    @property
    def url(self) -> str:
        """Getter для url (Только чтение)"""
        return self._url

    def get_attempts_data(
        self, client: str, client_key: str, start: str, end: str
    ) -> List[Dict[str, Any]]:
        """
        Извлекаем данные о попытках студентов с помощью API.
        """

        if not all([client, client_key, start, end]):
            error_msg = "Все параметры должны быть заполнены"
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        params = {
            "client": client,
            "client_key": client_key,
            "start": start,
            "end": end,
        }
        try:
            self._logger.info(f"Запрос данных за период {start} - {end}")

            response = requests.get(self._url, params=params, timeout=180)
            response.raise_for_status()

            attempts_data = response.json()
            self._logger.info(f"Успешно получено {len(attempts_data)} записей от API.")
            return attempts_data

        except requests.exceptions.Timeout as err:
            self._logger.error(f"Запрос к API выполнялся более 3 минут: {repr(err)}.")
            raise

        except requests.exceptions.HTTPError as err:
            self._logger.error(
                f"HTTP ошибка {err.response.status_code} при запросе к API: {repr(err)}."
            )
            raise

        except requests.exceptions.RequestException as err:
            self._logger.error(f"Ошибка соединения с API: {repr(err)}.")
            raise

        except Exception as err:
            self._logger.error(f"Ошибка при получении данных от API: {repr(err)}.")
            raise
