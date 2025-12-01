from typing import List
import logging
from datetime import timedelta
import smtplib
import ssl
from email.message import EmailMessage
from .logger_configs import setup_logging

setup_logging()


class EmailNotifier:
    """
    Класс для отправки email-уведомлений о попытках студентов.
    """

    def __init__(
        self,
        smtp_server: str,
        port: int,
        sender_email: str,
        sender_password: str,
        recipients: List[str],
    ):
        self._logger = logging.getLogger("EmailNotifier")
        self._smtp_server = smtp_server
        self._port = port
        self._sender_email = sender_email
        self._sender_password = sender_password
        self._recipients = recipients

    def send_success_report(
        self,
        api_records_cnt: int,
        processed_records_cnt: int,
        sheets_url: str,
        exec_time: timedelta = None,
    ) -> None:
        """
        Отправка отчета по email об успешном выполнении.
        Метод ничего не возвращает.
        """

        subject = "✅ Отчет о обработке данных LMS - УСПЕХ"

        # Убираем микросекунды для время выполнения
        exec_time_str = (
            str(exec_time).split(".", maxsplit=1)[0] if exec_time else "неизвестно"
        )

        body = (
            "Запуск скрипта выполнен успешно!\n"
            "\n"
            "📊 Статистика обработки:\n"
            f"✅ Получено записей из API: {api_records_cnt}.\n"
            f"✅ Обработано записей: {processed_records_cnt}.\n"
            f"⚠️ Пропущено записей: {api_records_cnt - processed_records_cnt}.\n"
            "✅ Записи успешно вставлены в БД!\n"
            "✅ Статистика отправлена в Google Sheets!\n"
            f"📈 Ссылка на Google Sheets: {sheets_url}\n"
            "\n"
            f"⏱ Время выполнения: {exec_time_str}\n"
            "🕐 Следующий запуск через 24 часа...\n"
            "\n"
            "---------------\n"
            "Автоматическое сообщение от системы мониторинга LMS."
        )

        return self._send_email(subject, body)

    def send_error_report(self, error_msg: str) -> None:
        """
        Отправка отчета по email об ошибке.
        Метод ничего не возвращает.
        """

        subject = "❌ Отчет о обработке данных LMS - ОШИБКА"

        body = (
            "ВНИМАНИЕ! При выполнении скрипта произошла ошибка!\n"
            "\n"
            f"🚨 Ошибка: {error_msg}.\n"
            "\n"
            "🔍 Требуется вмешательство разработчика!\n"
            "\n"
            "---------------\n"
            "Автоматическое сообщение от системы мониторинга LMS."
        )

        return self._send_email(subject, body)

    def _send_email(self, subject: str, body: str) -> None:
        """
        Метод для отправки email. Используется только внутри класса EmailNotifier.
        Ничего не возвращает.
        """
        try:
            # Создаем сообщение
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = self._sender_email
            msg["To"] = ", ".join(self._recipients)
            msg.set_content(body.strip())

            # Создаем SSL контекст
            context = ssl.create_default_context()

            # Отправляем email
            self._logger.info("Отправляем email...")
            with smtplib.SMTP_SSL(
                self._smtp_server, self._port, context=context
            ) as server:
                server.login(self._sender_email, self._sender_password)
                server.send_message(msg)

            self._logger.info(f"Email отправлен успешно. Тема: {subject}.")

        except smtplib.SMTPAuthenticationError as err:
            self._logger.error(
                f"Ошибка аутентификации. Проверьте email и пароль приложения: {repr(err)}."
            )
            raise

        except (smtplib.SMTPConnectError, ConnectionRefusedError, TimeoutError) as err:
            self._logger.error(f"Ошибка подключения к SMTP серверу: {repr(err)}")
            raise

        except Exception as err:
            self._logger.error(f"Ошибка при отправке email: {repr(err)}.")
            raise
