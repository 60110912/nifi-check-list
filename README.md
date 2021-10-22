# nifi-check-list
Проверяет Nifi файлы по видам проверок.
# Установка библиотек
```sh
pip3 install -r requirements.txt
``` 
# Пример использования
```sh
python3 check_nifi_file.py --file example/outlook_exchange.json
```
В результате проверки создается файл report.csv с логом всех ошибок.

# Пример проверки на nifi dev
## Пример конфигурации подключения к nifi
config_nifi.yml:
```
nifi_config:
  host: 'https://nifi.devdata.lmru.tech'
  username: '60110912'
  nifi_api: /nifi-api
  regestry_api: /nifi-registry-api
  nifi_web: /nifi
```
## Добавление пароля в файл
```sh
python3 encrypt_password.py --file config_nifi.yml --password ******
```
# Прохождение тестов
```sh
python3 nifi_get_data.py --id f80e9187-fc45-3e54-9207-b124432f6a62
```