from pathlib import Path
from datetime import datetime, timedelta
import logging

LOG_DIR = Path(__file__).parent.parent.parent / "logs"
SCRIPT_START_DATE = datetime.now().strftime("%Y-%m-%d")


def setup_logging() -> None:
    """Настраивает логирование."""
    # Создаем директорию для логов, если она не существует
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        filename=LOG_DIR / f"{SCRIPT_START_DATE}.log",
        filemode="a",
        encoding="utf-8",
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(name)s | %(asctime)s | %(levelname)s | %(message)s",
    )


def clean_old_logs(days: int = 9) -> None:
    """
    Удаляет старые логи.
    По умолчанию старыми считаются логи, которым 9 дней и более.
    """
    if days < 1:
        logging.warning(f"Удаление невозможно: days={days}. Должно быть >= 1.")
        return

    log_files = LOG_DIR.glob("*.log")
    cur_date = datetime.fromisoformat(SCRIPT_START_DATE)

    for log_file in log_files:
        try:
            dt = datetime.fromisoformat(log_file.stem)

            if cur_date - dt >= timedelta(days=days):
                logging.info(f"Удаляем файл: {log_file.name}.")
                log_file.unlink()  # Удаляем файл

        except ValueError as err:
            logging.error(f"Неверное имя файла: {log_file.name}. {repr(err)}.")
        except Exception as err:
            logging.error(f"Ошибка при удалении {log_file.name}: {repr(err)}.")
