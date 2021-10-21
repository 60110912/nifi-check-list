import re
import jsonpath
from jsonschema import validate, exceptions
from nifi_check_list.validation_shcema import nifiValidationShcemas
import pandas as pd
from nifi_check_list.NifiMyltyGraph import NifiMultyGraph
import logging

log = logging.getLogger("validate_nifi")


def makeErrorMessage(errorObject) -> str:
    """
    Функция формирует ответ ошибки несоответсвия схемы данных:
    Параметр:
        source - строка с непечатными символами
    """
    return str(
        str(errorObject.message)
        + " " + str(errorObject.validator)
        + " " + str(errorObject.validator_value)
        )


def checkAllProcessorsIsEnables(jsonobj) -> pd.DataFrame:
    """
    Функция проверяет, что все процессоры Enables:
    Параметр:
        jsonobj - схема nifi
    """
    testName = "Включены все процессоры"
    log.info(f'Проверяем пункт "{testName}"')
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    result = pd.DataFrame()
    for item in processors:
        try:
            validate(item, nifiValidationShcemas['processors_enabled'])
            log.info("Record #{}: OK".format(item['name']))
        except exceptions.ValidationError as ve:
            log.info("Record #{}: ERROR".format(item['name']))
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    makeErrorMessage(ve)
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
    log.info(f'Проверка закончена по пункту "{testName}"')
    return result


def checkAllProcessorValidName(jsonobj) -> pd.DataFrame:
    """
    Функция проверяет, что все процессоры начинаются одинаково:
    Параметр:
        jsonobj - схема nifi
    """
    testName = "Префикс системы"
    log.info(f'Проверяем пункт "{testName}"')
    result = pd.DataFrame()
    log.info("Определяем самый популярный префикс")
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    popular_prefix_names = {}
    for item in processors:
        prefix_name = item["name"].split("_")[0]
        if prefix_name in popular_prefix_names:
            popular_prefix_names[prefix_name] += 1
        else:
            popular_prefix_names[prefix_name] = 1
    popular_prefix = max(popular_prefix_names, key=popular_prefix_names.get)
    log.info(f'Популярный префикс {popular_prefix}')
    log.info('Формируем регулярное выражение')
    name_pattern = '^' + popular_prefix + "_"
    log.info('Проверяем имя каждого процессора на совпадение')
    for item in processors:
        if re.match(name_pattern, item['name']):
            log.info("Record #{}: OK".format(item['name']))
        else:
            log.info("Record #{}: ERROR".format(item['name']))
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    'Название процессора начинается не верно'
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
    log.info(f'Проверка закончена по пункту "{testName}"')
    return result


def checkConsumeKafkaRecor(g: NifiMultyGraph) -> pd.DataFrame:
    """
        Функция проверяет топологию графа.
        Параметр:
            g - объект класса NifiMultyGraph
    """
    testName = "Стандарт выходов из KafkaRecord"
    log.info(f'Проверяем пункт "{testName}"')
    return g.checkConsumeKafkaRecord()


def checkMergeContentBeforePut(g: NifiMultyGraph, jsonobj) -> pd.DataFrame:
    """
    Функция проверяет топологию графа и для выбранного Merge компонента проверяет параметры.
    Параметр:
            g - объект класса NifiMultyGraph
    """
    testName = "Merge компонента перед вставкой"
    result = pd.DataFrame()
    log.info(f'Проверяем правильность заполненности "{testName}"')
    testedMerge = g.selectMergeContentBeforePut()
    if testedMerge is None:
        log.info('Не найден Merge процесс перед вставкой в GP')
        return result
    log.info(f'Для идентификатора {testedMerge} получам объект')
    processors = jsonpath.jsonpath(jsonobj, f"$..processors[?(@.identifier == '{testedMerge}')]")

    for item in processors:
        try:
            identifier = item['identifier']
            log.info(f'Для идентификатора {identifier} проверяем заполнение параметров')
            validate(item, nifiValidationShcemas['MergeContent_before_Put'])
            log.info(f'Для идентификатора {identifier} параметры нормальные')
        except exceptions.ValidationError as ve:
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    makeErrorMessage(ve)
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
            log.info(f'Ошибка в настройках идентификатора {identifier}')
    return result


def checkSchemaObjects(jsonobj) -> pd.DataFrame:
    """
    Функция проверки схем у controller servece
    Параметр:
        jsonobj - схема nifi
    """
    testName = 'Валидация стандартов оформления ресурсов'
    log.info(f'Запускаем тест "{testName}"')
    resource = jsonpath.jsonpath(jsonobj, '$..processors.*')
    resource += jsonpath.jsonpath(jsonobj, '$..controllerServices.*')
    result = pd.DataFrame()
    for item in resource:
        try:
            (identifier, objectType) = (item['identifier'], item['type'])
            log.info(f'Для объекта {identifier} применяем схему валидации {objectType}')
            if item['type'] in nifiValidationShcemas:
                validate(item, nifiValidationShcemas[objectType])
                log.info("Record #{}: OK".format(item['name']))
            else:
                temp_result = pd.DataFrame(
                    [[
                        item['identifier'],
                        testName,
                        'Warning',
                        'Для ресурса нет схемы валидации'
                    ]],
                    columns=['Identifier', 'Tests name', 'Result', 'Message']
                )
                result = result.append(temp_result)
        except exceptions.ValidationError as ve:
            log.info("Record #{}: ERROR".format(item['name']))
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    makeErrorMessage(ve)
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
    log.info(f'Закончили тест "{testName}"')
    return result


# Преобразовать строку формата json в объект python
def getAllComponent(jsonobj, id=None) -> pd.DataFrame:
    """
    Функция выдает все объекты, которые есть в схеме:
    Параметр:
        jsonobj - схема nifi
    """
    all_node = jsonobj['flowContents']
    log.info('Получаем название Flow')
    result = pd.DataFrame(
                [[
                    all_node['identifier'],
                    all_node['name'],
                    all_node['componentType'],
                    all_node.get('type')
                ]],
                columns=['Identifier', 'name', 'componentType', 'type']
            )
    if id:
        df = pd.DataFrame(
                            [[
                                id,
                                "Process Group",
                                "Process Group",
                                "Process Group"
                            ]], columns=['Identifier', 'name', 'componentType', 'type']
                        )
        result = result.append(df)

    log.info('Получаем вложенных объектов во Flow')
    for item, value in all_node.items():
        if isinstance(value, type([])):
            all_sub = jsonpath.jsonpath(
                        jsonobj,
                        '$.flowContents.{item}.*'.format(item=item)
                        )
            if all_sub:
                for node in all_sub:
                    df = pd.DataFrame(
                            [[
                                node['identifier'],
                                node.get('name'),
                                node.get('componentType'),
                                node.get('type')
                            ]], columns=['Identifier', 'name', 'componentType', 'type']
                        )
                    result = result.append(df)
    log.info('Возвращаем датафрейм со всеми объектами')
    return result


def checkAllProcessorsIsNormal(jsonobj) -> pd.DataFrame:
    """
    Функция проверяет, что все процессоры включены:
    Параметр:
        jsonobj - схема nifi
    """
    testName = "Включены все процессоры и нет неактивных"
    result = pd.DataFrame()
    item = jsonobj
    log.info(f'Проверяем пункт "{testName}"')
    try:
        validate(jsonobj, nifiValidationShcemas['process_group_check_status'])
        log.info("Record #{}: OK".format(item['id']))
    except exceptions.ValidationError as ve:
        log.info("Record #{}: ERROR".format(item['id']))
        temp_result = pd.DataFrame(
            [[
                item['id'],
                testName,
                'Error',
                makeErrorMessage(ve)
            ]],
            columns=['Identifier', 'Tests name', 'Result', 'Message']
        )
        result = result.append(temp_result)
    log.info(f'Проверка закончена по пункту "{testName}"')
    return result
