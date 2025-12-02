from typing import Dict, Any, List, Tuple
from datetime import datetime
import ast
import logging
from .logger_configs import setup_logging

setup_logging()


class DataProcessor:
    """
    Класс для обработки и валидации данных из API.
    """

    _logger = logging.getLogger("DataProcessor")

    @staticmethod
    def processing_attempts(attempts_data: List[Dict[str, Any]]) -> List[Tuple]:
        """
        Обрабатывает сырые данные из API и возвращает список кортежей для вставки в БД.
        """
        processed_attempts = []
        total_records = len(attempts_data)

        if total_records == 0:
            DataProcessor._logger.info("Нет данных для обработки.")
            return processed_attempts

        DataProcessor._logger.info(f"Начало обработки {total_records} записей.")

        for i, attempt in enumerate(attempts_data):
            try:
                valid_attempt = DataProcessor._validate_attempt(attempt)
                processed_attempts.append(valid_attempt)

            except ValueError as err:
                DataProcessor._logger.warning(f"Пропущена запись {i + 1}: {err}")

            except Exception as err:
                DataProcessor._logger.error(
                    f"Непредвиденная ошибка в записи {i + 1}: {repr(err)}."
                )

        success_records = len(processed_attempts)
        DataProcessor._logger.info(
            f"Обработка завершена. Успешно: {success_records}, пропущено: {total_records - success_records}."
        )

        return processed_attempts

    @staticmethod
    def _validate_attempt(attempt: Dict[str, Any]) -> Tuple:
        """
        Обрабатывает и валидирует одну запись.
        Выбрасывает исключение ValueError, если запись невалидна.
        Используется только внутри класса DataProcessor.
        """

        # Извлекаем данные
        user_id = attempt.get("lti_user_id")
        attempt_type = attempt.get("attempt_type")
        is_correct_int = attempt.get("is_correct")
        created_at_str = attempt.get("created_at")
        passback_params_str = attempt.get("passback_params")

        # Проверка обязательных полей
        if not user_id:
            raise ValueError("Отсутствует user_id.")
        if not attempt_type:
            raise ValueError("Отсутствует attempt_type.")
        if not created_at_str:
            raise ValueError("Отсутствует created_at.")
        if not passback_params_str:
            raise ValueError("Отсутствует passback_params.")

        # Валидация user_id
        if not isinstance(user_id, str):
            raise ValueError("user_id должен быть строкой.")

        # Валидация is_correct
        if is_correct_int not in (0, 1, None):
            raise ValueError("is_correct должен быть 0, 1 или None.")

        is_correct = bool(is_correct_int) if is_correct_int is not None else None

        # Валидация attempt_type
        if attempt_type not in ("run", "submit"):
            raise ValueError("attempt_type должен быть 'run' или 'submit'.")

        # Парсинг даты
        try:
            created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError as err:
            raise ValueError(f"Неверный формат created_at: {err}.")

        # Парсинг passback_params
        try:
            passback_dict = ast.literal_eval(passback_params_str)
        except (SyntaxError, ValueError) as err:
            raise ValueError(f"Ошибка парсинга passback_params: {repr(err)}.")

        # Извлечение полей из passback_params
        oauth_consumer_key = passback_dict.get("oauth_consumer_key")
        lis_result_sourcedid = passback_dict.get("lis_result_sourcedid")
        lis_outcome_service_url = passback_dict.get("lis_outcome_service_url")

        # Валидация полей из passback_params
        for param_name, param_value in [
            ("oauth_consumer_key", oauth_consumer_key),
            ("lis_result_sourcedid", lis_result_sourcedid),
            ("lis_outcome_service_url", lis_outcome_service_url),
        ]:
            if not isinstance(param_value, str) and not param_value is None:
                raise ValueError(f"{param_name} должен быть строкой или None")

        # Возвращаем кортеж из валидных полей
        return (
            user_id,
            oauth_consumer_key,
            lis_result_sourcedid,
            lis_outcome_service_url,
            is_correct,
            attempt_type,
            created_at,
        )

    @staticmethod
    def get_cols() -> List[str]:
        """Возвращает список с названиями столбцов."""
        return [
            "user_id",
            "oauth_consumer_key",
            "lis_result_sourcedid",
            "lis_outcome_service_url",
            "is_correct",
            "attempt_type",
            "created_at",
        ]
