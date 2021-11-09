"""
В модуле собраны скрипты общего плана.
"""
import json


def utf8(s: bytes):
    """
    Функция печати потока байт в utf-8.
    Параметр:
        s - поток байт
    """
    return str(s, 'utf-8')


def unload_error_json(fileName, jsonObgect):
    """
    Функция json выводит в файл
    Параметры:
        fileName - название файла
        jsonJbgect - json объект
    """
    with open(f'error/{fileName}.json', 'w') as file:
        json.dump(obj=jsonObgect, fp=file, indent=4)


def unload_error_csv(fileName, error, mode="w"):
    """
    Функция json выводит в файл
    Параметры:
        fileName - название файла
        jsonJbgect - json объект
    """
    with open(f'{fileName}.csv', mode=mode, encoding='utf-8') as file:
        file.write(error)
