from typing import List, Tuple, Dict, Any
import logging
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from .data_processor import DataProcessor
from .logger_configs import setup_logging

setup_logging()


class GoogleSheetsReporter:
    """
    Класс для записи статистики в Google Sheets.
    """

    def __init__(self, credentials_dict: Dict[str, Any], spreadsheet_id: str):
        self._logger = logging.getLogger("GoogleSheetsReporter")

        try:
            self._logger.info("Подключаемся к Google Sheets...")

            creds = Credentials.from_service_account_info(
                credentials_dict,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            client = gspread.authorize(creds)

            self._spreadsheet = client.open_by_key(spreadsheet_id)
            self._sheet1 = self._spreadsheet.sheet1

            self._logger.info("Успешное подключение к Google Sheets.")

        except FileNotFoundError as err:
            self._logger.error(f"Файл с данными не найден: {repr(err)}.")
            raise

        except gspread.exceptions.SpreadsheetNotFound as err:
            self._logger.error(
                f"Таблица с ID {spreadsheet_id} не найдена: {repr(err)}."
            )
            raise

        except gspread.exceptions.APIError as err:
            self._logger.error(f"Ошибка API Google Sheets: {repr(err)}.")
            raise

        except Exception as err:
            self._logger.error(
                f"Неожиданная ошибка при подключении к Google Sheets: {repr(err)}."
            )
            raise

    def append_stats(self, processed_attempts: List[Tuple]) -> None:
        """
        Метод добавляет статистику по попыткам в Google Sheets.
        Ничего не возвращает.
        """
        if not processed_attempts:
            self._logger.info("Нет данных для записи статистики.")
            return

        try:
            self._logger.info(
                f"Начало подсчета статистики для {len(processed_attempts)} записей."
            )

            headers = [
                "Дата",
                "Всего попыток",
                "Уникальные пользователи",
                "Успешные попытки",
                "(%) успешных попыток",
                "Запускали код",
                "Проверяли код",
            ]

            # Проверяем и обновляем заголовки
            if self._sheet1.row_values(1) != headers:
                self._logger.info("Обновляем заголовки таблицы.")
                self._sheet1.update([headers], "A1:G1")

            # Создаем DataFrame
            attempts_df = pd.DataFrame(
                processed_attempts, columns=DataProcessor.get_cols()
            )

            # Устанавливаем индекс для resample
            attempts_df.set_index("created_at", inplace=True)

            # Агрегируем данные по дням
            # Считаем сколько попыток было совершено, сколько было успешных и какое количество уникальных пользователей
            stats_df = (
                attempts_df.resample("D")
                .agg(
                    {"attempt_type": "count", "user_id": "nunique", "is_correct": "sum"}
                )
                .reset_index()
                .rename(
                    columns={
                        "attempt_type": "total_attempts",
                        "user_id": "unique_users",
                        "is_correct": "successful_attempts",
                    }
                )
            )

            # Количество по типам попыток
            stats_df = stats_df.merge(
                attempts_df.resample("D")["attempt_type"]
                .agg(
                    run_cnt=lambda x: (x == "run").sum(),
                    submit_cnt=lambda x: (x == "submit").sum(),
                )
                .reset_index(),
                on="created_at",
            )

            # Процент успешных submit-попыток с защитой от деления на ноль
            stats_df["success_rate"] = np.where(
                stats_df["submit_cnt"] > 0,
                (stats_df["successful_attempts"].mul(100) / stats_df["submit_cnt"])
                .astype("float64")
                .round(2),
                0,
            )

            # Задаем нужный порядок столбцов
            stats_df = stats_df[
                [
                    "created_at",
                    "total_attempts",
                    "unique_users",
                    "successful_attempts",
                    "success_rate",
                    "run_cnt",
                    "submit_cnt",
                ]
            ]

            # Форматируем дату
            stats_df["created_at"] = stats_df["created_at"].dt.strftime("%Y-%m-%d")

            # Добавляем статистику в Google Таблицу
            self._sheet1.append_rows(stats_df.values.tolist())

            self._logger.info(
                f"Успешно добавлена статистика {stats_df['total_attempts'].sum()} попыток за {len(stats_df)} день."
            )

        except Exception as err:
            self._logger.error(f"Ошибка при записи статистики: {repr(err)}.")
            raise
