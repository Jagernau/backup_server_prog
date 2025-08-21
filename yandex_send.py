import yadisk
from dotenv import dotenv_values
env_dict = dotenv_values('.env')
DISK_TOKEN: str = str(env_dict["DISK_TOKEN"])
# Создание клиента с токеном
client = yadisk.Client(token=DISK_TOKEN)

# Проверка валидности токена
if client.check_token():
    print("Токен действителен")
    items = client.listdir("/")
    folders = [item['name'] for item in items if item['type'] == 'dir']
    print('Папки на Яндекс Диске:')
    for folder in folders:
        print(folder)
else:
    print("Токен недействителен")

client.close()

def upload_to_yandex_disk(backup_file):
    # Загружаем на Яндекс.Диск
    try:
        client.upload(backup_file, YANDISK_PATH + os.path.basename(backup_file))
        print(f"Бэкап {backup_file} успешно загружен на Яндекс.Диск.")
        return True
    except Exception as e:
        print(f"Ошибка при загрузке бэкапа на Яндекс.Диск: {e}")
        return False
