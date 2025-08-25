import os
import subprocess
import tarfile
import yadisk
import schedule
import time
from datetime import datetime
import logging
import traceback

from dotenv import dotenv_values
env_dict = dotenv_values('.env')

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

ENCRYPTION_PASSWORD:str = str(env_dict["ENCRYPTION_PASSWORD"])

def encrypt_file(input_file, output_file):
    try:
        subprocess.run(
            ["openssl", "enc", "-aes-256-cbc", "-salt", "-in", input_file, "-out", output_file, "-pass", f"pass:{ENCRYPTION_PASSWORD}"],
            check=True
        )
        logger.info(f"Файл {input_file} зашифрован и сохранён как {output_file}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при шифровании: {e}")

# Функция для расшифровки файла
def decrypt_file(input_file, output_file):
    try:
        subprocess.run(
            ["openssl", "enc", "-d", "-aes-256-cbc", "-in", input_file, "-out", output_file, "-pass", f"pass:{ENCRYPTION_PASSWORD}"],
            check=True
        )
        logger.info(f"Файл {input_file} расшифрован и сохранён как {output_file}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при расшифровке: {e}")

# Функция для создания LVM снапшота
def create_lvm_snapshot():
    snapshot_name = SNAPSHOT_NAME
    snapshot_path = f"/dev/{LVM_VOLUME_GROUP}/{snapshot_name}"
    snapshot_command = [
        LVM_COMMAND,
        "-L", SNAPSHOT_SIZE,
        "-s",
        "-n", snapshot_name,
        f"/dev/{LVM_VOLUME_GROUP}/{LVM_LOGICAL_VOLUME}"
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
    DISK_TOKEN: str = str(env_dict["DISK_TOKEN"])
    # Создание клиента с токеном
    y = yadisk.YaDisk(token=DISK_TOKEN)

    encrypted_archive = f"{archive_name}.enc"
    encrypt_file(archive_name, encrypted_archive)

    remote_path = os.path.join(YANDEX_DIR, os.path.basename(archive_name))

    # Загружаем файл на Яндекс.Диск
    with open(encrypted_archive, 'rb') as file:
        y.upload(file, remote_path, timeout=1000)

    # Удаляем локальный файл после успешной загрузки
    os.remove(archive_name)

# Функция для удаления старых бэкапов на Яндекс.Диске
def clean_old_backups():
    DISK_TOKEN: str = str(env_dict["DISK_TOKEN"])
    y = yadisk.YaDisk(token=DISK_TOKEN)
    remote_files = list(y.listdir(YANDEX_DIR))

    # Сортируем по дате создания
    remote_files = sorted(remote_files, key=lambda f: f['modified'], reverse=True)

    # Оставляем только 3 последних
    files_to_delete = remote_files[3:]

    for file in files_to_delete:
        y.remove(file['path'])

# Функция для скачивания и расшифровки файла с Яндекс.Диска
def download_from_yandex(remote_path, local_path):
    # Создание клиента с токеном
    env_dict = dotenv_values('.env')
    DISK_TOKEN = str(env_dict["DISK_TOKEN"])
    y = yadisk.YaDisk(token=DISK_TOKEN)

    # Скачиваем зашифрованный файл
    with open(local_path, 'wb') as file:
        y.download(remote_path, file)

    # Расшифровываем файл
    decrypted_file = local_path.replace(".enc", "")
    decrypt_file(local_path, decrypted_file)

    # Удаляем зашифрованный файл
    os.remove(local_path)

    # Разархивируем
    subprocess.run(["tar", "-xzvf", decrypted_file], check=True)
    os.remove(decrypted_file)

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
#schedule.every().tuesday.at("02:00").do(backup)
schedule.every().friday.at("09:48").do(backup)

# Главный цикл для запуска планировщика
if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)
