import re
import jsonpath
from jsonschema import validate, exceptions
from nifi_check_list.validation_shcema import nifiValidationShcemas
import pandas as pd
from nifi_check_list.NifiMyltyGraph import NifiMultyGraph
import logging

log = logging.getLogger("validate_nifi")


def checkAllProcessorsIsEnables(jsonobj) -> pd.DataFrame:
    """
    Функция проверяет, что все процессоры включены:
    Параметр:
        jsonobj - схема nifi
    """
    testName = "Включены все процессоры"
    log.debug(f'Проверяем пункт "{testName}"')
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    result = pd.DataFrame()
    for item in processors:
        try:
            validate(item, nifiValidationShcemas['processors_enabled'])
            log.debug("Record #{}: OK".format(item['name']))
        except exceptions.ValidationError as ve:
            log.debug("Record #{}: ERROR".format(item['name']))
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    ve
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
    log.debug(f'Проверка закончена по пункту "{testName}"')
    return result


def checkAllProcessorValidName(jsonobj) -> pd.DataFrame:
    """
    Функция проверяет, что все процессоры начинаются одинаково:
    Параметр:
        jsonobj - схема nifi
    """
    testName = "Префикс системы"
    log.debug(f'Проверяем пункт "{testName}"')
    result = pd.DataFrame()
    name_pattern = jsonpath.jsonpath(jsonobj, '$.flowContents.name')[0]
    name_pattern = '^' + name_pattern
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    for item in processors:
        if re.match(name_pattern, item['name']):
            log.debug("Record #{}: OK".format(item['name']))
        else:
            log.debug("Record #{}: ERROR".format(item['name']))
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
    log.debug(f'Проверка закончена по пункту "{testName}"')
    return result


def checkConsumeKafkaRecor(g: NifiMultyGraph) -> pd.DataFrame:
    """
        Функция проверяет топологию графа.
        Параметр:
            g - объект класса NifiMultyGraph
    """
    testName = "Стандарт выходов из KafkaRecord"
    log.debug(f'Проверяем пункт "{testName}"')
    return g.checkConsumeKafkaRecord()


def checkMergeContentBeforePut(g: NifiMultyGraph, jsonobj) -> pd.DataFrame:
    """
    Функция проверяет топологию графа и для выбранного Merge компонента проверяет параметры.
    Параметр:
            g - объект класса NifiMultyGraph
    """
    testName = "Merge компонента перед вставкой"
    result = pd.DataFrame()
    log.debug(f'Проверяем правильность заполненности "{testName}"')
    testedMerge = g.selectMergeContentBeforePut()
    log.debug(f'Для идентификатора {testedMerge} получам объект')
    processors = jsonpath.jsonpath(jsonobj, f"$..processors[?(@.identifier == '{testedMerge}')]")
    for item in processors:
        try:
            identifier = item['identifier']
            log.debug(f'Для идентификатора {identifier} проверяем заполнение параметров')
            validate(item, nifiValidationShcemas['MergeContent_before_Put'])
            log.debug(f'Для идентификатора {identifier} параметры нормальные')
        except exceptions.ValidationError as ve:
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    ve
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
            log.debug(f'Ошибка в настройках идентификатора {identifier}')
    return result


def checkSchemaObjects(jsonobj) -> pd.DataFrame:
    """
    Функция проверки схем у controller servece
    Параметр:
        jsonobj - схема nifi
    """
    testName = 'Валидация стандартов оформления ресурсов'
    log.debug(f'Запускаем тест "{testName}"')
    resource = jsonpath.jsonpath(jsonobj, '$..processors.*')
    resource += jsonpath.jsonpath(jsonobj, '$..controllerServices.*')
    result = pd.DataFrame()
    for item in resource:
        try:
            (identifier, objectType) = (item['identifier'], item['type'])
            log.debug(f'Для объекта {identifier} применяем схему валидации {objectType}')
            if item['type'] in nifiValidationShcemas:
                validate(item, nifiValidationShcemas[objectType])
                log.debug("Record #{}: OK".format(item['name']))
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
            log.debug("Record #{}: ERROR".format(item['name']))
            temp_result = pd.DataFrame(
                [[
                    item['identifier'],
                    testName,
                    'Error',
                    ve
                ]],
                columns=['Identifier', 'Tests name', 'Result', 'Message']
            )
            result = result.append(temp_result)
    log.debug(f'Закончили тест "{testName}"')
    return result


# Преобразовать строку формата json в объект python
def getAllComponent(jsonobj) -> pd.DataFrame:
    """
    Функция выдает все объекты, которые есть в схеме:
    Параметр:
        jsonobj - схема nifi
    """
    all_node = jsonobj['flowContents']
    log.debug('Получаем название Flow')
    result = pd.DataFrame(
                [[
                    all_node['identifier'],
                    all_node['name'],
                    all_node['componentType'],
                    all_node.get('type')
                ]],
                columns=['Identifier', 'name', 'componentType', 'type']
            )
    log.debug('Получаем вложенных объектов во Flow')
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
                                node['name'],
                                node['componentType'],
                                node.get('type')
                            ]], columns=['Identifier', 'name', 'componentType', 'type']
                        )
                    result = result.append(df)
    log.debug('Возвращаем датафрейм со всеми объектами')
    return result



