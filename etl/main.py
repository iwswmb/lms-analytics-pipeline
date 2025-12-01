import os
import base64
import json
import logging
from typing import Tuple
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from components import APIClient, DataProcessor, DatabaseInserter
from components import GoogleSheetsReporter, EmailNotifier
from components import setup_logging, clean_old_logs


setup_logging()
clean_old_logs()
logger = logging.getLogger("Main")


def get_date_range() -> Tuple[str, str]:
    """
    Генерирует даты начала и окончания для получения данных по API.
    Возвращает кортеж из 2 дат в строковом формате.
    """
    cur_date = datetime.now()

    start_date = (cur_date - timedelta(days=3)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    end_date = (cur_date - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    return (
        start_date.strftime("%Y-%m-%d %H:%M:%S.%f"),
        end_date.strftime("%Y-%m-%d %H:%M:%S.%f"),
    )


def get_email_notifier() -> EmailNotifier:
    """Возвращает объект класса EmailNotifier."""
    return EmailNotifier(
        smtp_server=os.getenv("SMTP_SERVER"),
        port=465,
        sender_email=os.getenv("SENDER_EMAIL"),
        sender_password=os.getenv("SENDER_PASSWORD"),
        recipients=os.getenv("RECIPIENTS_EMAILS").split(","),
    )


def main():
    """Главная функция ETL-процесса."""
    logger.info("Запуск ETL-процесса...")

    email_notifier = None
    db_inserter = None

    try:
        start_time = datetime.now()

        # Загрузка переменных окружения
        load_dotenv(find_dotenv())

        # Получение данных
        api_client = APIClient(url=os.getenv("API_URL"))
        start, end = get_date_range()
        attempts_data = api_client.get_attempts_data(
            client=os.getenv("API_CLIENT"),
            client_key=os.getenv("API_CLIENT_KEY"),
            start=start,
            end=end,
        )

        # Обработка данных
        processed_attempts = DataProcessor.processing_attempts(attempts_data)

        # Вставка в БД
        db_inserter = DatabaseInserter(
            host=os.getenv("DB_HOST"),
            port=5432,
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        db_inserter.insert_attempts(processed_attempts)

        # Отправка статистики в Google Sheets
        credentials_json = base64.b64decode(
            os.getenv("GOOGLE_SHEETS_CREDENTIALS_BASE64")
        ).decode("utf-8")
        sheets_reporter = GoogleSheetsReporter(
            credentials_dict=json.loads(credentials_json),
            spreadsheet_id=os.getenv("SPREADSHEET_ID"),
        )
        sheets_reporter.append_stats(processed_attempts)

        # Отправка email об успехе
        email_notifier = get_email_notifier()
        sheets_url = f"https://docs.google.com/spreadsheets/d/{os.getenv('SPREADSHEET_ID')}/edit?usp=sharing"
        exec_time = datetime.now() - start_time
        email_notifier.send_success_report(
            api_records_cnt=len(attempts_data),
            processed_records_cnt=len(processed_attempts),
            sheets_url=sheets_url,
            exec_time=exec_time,
        )

        logger.info("ETL-процесс успешно завершен.")

    except Exception as err:
        error_msg = f"Ошибка в ETL-процессе: {repr(err)}"
        logger.error(error_msg)

        try:
            if not email_notifier:
                email_notifier = get_email_notifier()
            email_notifier.send_error_report(error_msg=error_msg)
        except Exception as email_err:
            logger.error(f"Не удалось отправить email об ошибке: {repr(email_err)}")

    finally:
        if db_inserter:
            try:
                db_inserter.close_connection()
            except Exception as err:
                logger.error(f"Ошибка при закрытии соединения с БД: {repr(err)}")


if __name__ == "__main__":
    main()
