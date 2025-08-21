import os
import subprocess
import tarfile
import yadisk
import schedule
import time
from datetime import datetime
import logging
import traceback

# Настройка логирования 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Изменяем на DEBUG для большей детализации

# Создаем форматтер с дополнительной информацией
formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            )

# Обработчик для файла
file_handler = logging.FileHandler('log_crud_objects.txt')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Конфигурация
LVM_VOLUME_GROUP = "ubuntu-vg"
LVM_LOGICAL_VOLUME = "ubuntu-lv"
SNAPSHOT_SIZE = "60G"  # Размер снапшота
SNAPSHOT_NAME = "backup_snapshot"
BACKUP_DIR = "/tmp/backups"
YANDEX_DIR = "/server_backups"  # Папка на Яндекс.Диске

# Путь к инструментам
LVM_COMMAND = "/usr/sbin/lvcreate"
TAR_COMMAND = "/bin/tar"

# Функция для создания LVM снапшота
def create_lvm_snapshot():
    snapshot_path = os.path.join(BACKUP_DIR, f"{SNAPSHOT_NAME}.snap")
    snapshot_command = [
        LVM_COMMAND,
        "-L", SNAPSHOT_SIZE,
        "-s", f"/dev/{LVM_VOLUME_GROUP}/{LVM_LOGICAL_VOLUME}",
        f"{snapshot_path}"
    ]
    subprocess.run(snapshot_command, check=True)
    return snapshot_path

# Функция для архивирования снапшота
def archive_snapshot(snapshot_path):
    archive_name = f"{snapshot_path}.tar.gz"
    with tarfile.open(archive_name, "w:gz") as tar:
        tar.add(snapshot_path, arcname=os.path.basename(snapshot_path))
    return archive_name

# Функция для загрузки на Яндекс.Диск
def upload_to_yandex(archive_name):
    from dotenv import dotenv_values
    env_dict = dotenv_values('.env')
    DISK_TOKEN: str = str(env_dict["DISK_TOKEN"])
    # Создание клиента с токеном
    y = yadisk.YaDisk(token=DISK_TOKEN)
    remote_path = os.path.join(YANDEX_DIR, os.path.basename(archive_name))
    
    # Загружаем файл на Яндекс.Диск
    with open(archive_name, 'rb') as file:
        y.upload(file, remote_path, timeout=1000)
    
    # Удаляем локальный файл после успешной загрузки
    os.remove(archive_name)

# Функция для удаления старых бэкапов на Яндекс.Диске
def clean_old_backups():
    from dotenv import dotenv_values
    env_dict = dotenv_values('.env')
    DISK_TOKEN: str = str(env_dict["DISK_TOKEN"])
    y = yadisk.YaDisk(token=DISK_TOKEN)
    remote_files = list(y.listdir(YANDEX_DIR))
    
    # Сортируем по дате создания
    remote_files = sorted(remote_files, key=lambda f: f['modified'], reverse=True)
    
    # Оставляем только 3 последних
    files_to_delete = remote_files[3:]
    
    for file in files_to_delete:
        y.remove(file['path'])

# Основная функция для создания и загрузки бэкапа
def backup():
    print(f"Начинаем создание бэкапа: {datetime.now()}")
    logger.info(f"Началось работы программы")
    # Создание снапшота
    try:
        logger.info(f"Началось создание снапшота")
        snapshot_path = create_lvm_snapshot()
    except Exception as e:
        logger.error(f"Ошибка при создании снапшота: {e}")
        logger.error(f"Ошибка при создании снапшота: {traceback.format_exc()}")
        logger.error(f"Ошибка при создании снапшота: {traceback.format_stack()}")
    else:
        logger.info(f"Снапшот успешно создан")
        try:
            logger.info(f"Началось архивирование снимка")
            # Архивирование снапшота
            archive_name = archive_snapshot(snapshot_path)
        except Exception as e:
            logger.error(f"Ошибка при архивирование снимка: {e}")
            logger.error(f"Ошибка при архивирование снимка: {traceback.format_exc()}")
            logger.error(f"Ошибка при архивирование снимка: {traceback.format_stack()}")
        else:
            logger.info(f"Снапшот успешно заархивирован")
            try:
                logger.info(f"Началась отправка на Яндекс диск")
                # Загрузка на Яндекс.Диск
                upload_to_yandex(archive_name)
            except Exception as e:
                logger.error(f"Ошибка при отправка на Яндекс диск: {e}")
                logger.error(f"Ошибка при отправка на Яндекс диск: {traceback.format_exc()}")
                logger.error(f"Ошибка при отправка на Яндекс диск: {traceback.format_stack()}")
            else:
                logger.info(f"Снапшот успешно отправлен на Яндекс диск")
                try:
                    logger.info(f"Началось удаление снапшотов с ЯД и Сервера")
                    # Удаление старых бэкапов на Яндекс.Диске
                    clean_old_backups()
                    # Удаление локального снапшота
                    subprocess.run(["lvremove", "-f", snapshot_path], check=True)
                    print(f"Бэкап успешно завершён: {datetime.now()}")
                except Exception as e:
                    logger.error(f"Ошибка при удаление снапшотов с ЯД и Сервера: {e}")
                    logger.error(f"Ошибка при удаление снапшотов с ЯД и Сервера: {traceback.format_exc()}")
                    logger.error(f"Ошибка при удаление снапшотов с ЯД и Сервера: {traceback.format_stack()}")
                else:
                    logger.info(f"Все операции отработали. Конец")

# Планировщик задач для выполнения по вторникам и пятницам в 02:00
schedule.every().tuesday.at("02:00").do(backup)
schedule.every().friday.at("02:00").do(backup)

# Главный цикл для запуска планировщика
if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)

