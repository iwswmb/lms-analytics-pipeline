from typing import List, Tuple
import logging
import psycopg2
from psycopg2.extras import execute_batch
from .logger_configs import setup_logging

setup_logging()


class DatabaseInserter:
    """
    Класс для вставки обработанных данных в БД.
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        if not hasattr(self, "_initialized"):

            self._logger = logging.getLogger("DatabaseInserter")

            try:
                self._logger.info("Установка подключения к БД...")

                self._connection = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                )
                self._initialized = True

                self._logger.info("Подключение к БД установлено.")

            except psycopg2.Error as err:
                self._logger.error(f"Ошибка подключения к БД: {repr(err)}.")
                raise

    def insert_attempts(self, processed_attempts: List[Tuple]) -> None:
        """
        Метод вставляет попытки студентов в базу данных.
        Ничего не возвращает.
        """
        if not processed_attempts:
            self._logger.info("Нет данных для вставки")
            return

        query = """
        INSERT INTO attempts (
            user_id,
            oauth_consumer_key,
            lis_result_sourcedid, 
            lis_outcome_service_url,
            is_correct,
            attempt_type,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT unique_attempt DO NOTHING
        """
        try:
            self._logger.info(f"Начало вставки {len(processed_attempts)} записей в БД.")

            with self._connection.cursor() as cursor:
                execute_batch(cursor, query, processed_attempts)
                self._connection.commit()

            self._logger.info("Записи успешно вставлены.")

        except psycopg2.Error as err:
            self._logger.error(f"Ошибка при вставке данных: {repr(err)}.")
            self._logger.info("Откат транзакции...")
            self._connection.rollback()
            raise

    def close_connection(self) -> None:
        """Закрывает соединение с БД"""
        if hasattr(self, "_connection") and not self._connection.closed:
            self._connection.close()
            self._logger.info("Соединение с БД закрыто.")

    def __del__(self):
        self.close_connection()
