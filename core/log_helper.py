import logging
import os
from pathlib import Path
from pythonjsonlogger import jsonlogger


def add_session_file_handler(logger: logging.Logger, session_id: str, request_id: str):
    log_dir = f"deployment/logs/req_{request_id}"
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"session_{session_id}.log")

    handler = logging.FileHandler(log_file, encoding='utf-8')
    # formatter = jsonlogger.JsonFormatter("%(session_id)s %(request_id)s %(levelname)s %(name)s %(lineno)d %(asctime)s %(message)s",datefmt='%Y-%m-%dT%H:%M:%S%z')
    formatter = logging.Formatter('[%(levelname)s|%(name)s|L%(lineno)d] %(asctime)s | %(message)s',datefmt='%Y-%m-%dT%H:%M:%S%z')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    return handler


def remove_session_file_handler(logger: logging.Logger, handler: logging.Handler):
    handler.close()
    logger.removeHandler(handler)


def delete_all_files(folder_path: str):
    """
    Deletes all files under the given folder (recursively).
    Does NOT delete directories.
    """
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Path does not exist: {folder}")

    if not folder.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder}")

    deleted_files = []

    for root, _, files in os.walk(folder):
        for file in files:
            file_path = Path(root) / file
            try:
                file_path.unlink()
                deleted_files.append(str(file_path))
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")

    return deleted_files


