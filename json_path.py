import re
import jsonpath
import json
from jsonschema import validate, exceptions  # ,  Draft201909Validator
from validation_shcema import nifiValidationShcemas
import pandas as pd
from NifiMyltyGraph import NifiMultyGraph
import logging
import click

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log = logging.getLogger("validate_nifi")


def checkAllProcessorsIsEnables(jsonobj):
    """
    Функция проверяет, что все процессоры включены:
    Параметр:
        jsonobj - схема nifi
    """
    log.debug('Проверяем пункт "Включены все процессоры"')
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    for idx, item in enumerate(processors):
        try:
            validate(item, nifiValidationShcemas['processors_enabled'])
            print("Record #{}: OK\n".format(item['name']))
        except exceptions.ValidationError as ve:
            print(ve)
            print("Record #{}: ERROR\n".format(item['name']))
    log.debug('Проверка закончена по пункту "Включены все процессоры"')


def checkAllProcessorValidName(jsonobj):
    """
    Функция проверяет, что все процессоры начинаются одинаково:
    Параметр:
        jsonobj - схема nifi
    """
    log.debug('Проверяем пункт "Префикс системы"')
    name_pattern = jsonpath.jsonpath(jsonobj, '$.flowContents.name')[0]
    name_pattern = '^' + name_pattern
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    for item in processors:
        if re.match(name_pattern, item['name']):
            print("Record #{}: OK\n".format(item['name']))
        else:
            print("Record #{}: ERROR\n".format(item['name']))
    log.debug('Проверка закончена по пункту "Префикс системы"')


def checkConsumeKafkaRecor(g: NifiMultyGraph) -> pd.DataFrame:
    """
        Функция проверяет топологию графа.
        Параметр:
            g - объект класса NifiMultyGraph
    """
    log.debug('Проверяем пункт "Стандарт выходов из KafkaRecord"')
    return g.checkConsumeKafkaRecord()


def checkMergeContentBeforePut(g: NifiMultyGraph, jsonobj):
    """
    Функция проверяет топологию графа и для выбранного Merge компонента проверяет параметры.
    Параметр:
            g - объект класса NifiMultyGraph
    """
    log.debug('Проверяем правильность заполненности "Merge компонента"')
    testedMerge = g.selectMergeContentBeforePut()
    log.debug(f'Для идентификатора {testedMerge} получам объект')
    processors = jsonpath.jsonpath(jsonobj, f"$..processors[?(@.identifier == '{testedMerge}')]")
    for idx, item in enumerate(processors):
        try:
            identifier = item['identifier']
            log.debug(f'Для идентификатора {identifier} проверяем заполнение параметров')
            validate(item, nifiValidationShcemas['MergeContent_before_Put'])
            log.debug(f'Для идентификатора {identifier} параметры нормальные')
        except exceptions.ValidationError as ve:
            log.error(ve)
            log.debug(f'Ошибка в настройках идентификатора {identifier}')


def checkSchemaController(jsonobj):
    """
    Функция проверки схем у controller servece
    Параметр:
        jsonobj - схема nifi
    """
    pass


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
                    all_node['componentType']
                ]],
                columns=['identifier', 'name', 'componentType'],
                index=['identifier']
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
                                node['componentType']
                            ]], columns=['identifier', 'name', 'componentType'],
                            index=['identifier']
                        )
                    result = result.append(df)
    log.debug('Возвращаем датафрейм со всеми объектами')
    return result


@click.command()
@click.option('--file',  help='Validate file must be a json struct')
@click.option(
    '--log',
    default="DEBUG",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    help='Log level. Default log level DEBUG')
def main(file, log):
    logging.basicConfig(
        level=log,
        format=FORMAT
    )

    with open(file) as json_file:
        data = json_file.read()
    jsonobj = json.loads(data)
    print(getAllComponent(jsonobj))
    checkAllProcessorsIsEnables(jsonobj)
    checkAllProcessorValidName(jsonobj)
    g = NifiMultyGraph()
    g.nifiSchemaLoad(jsonobj)
    test = g.checkConsumeKafkaRecord()
    print(test)
    print(g.selectMergeContentBeforePut())
    checkMergeContentBeforePut(g, jsonobj)


if __name__ == "__main__":
    main()


# Тест
# with open('unit_test/org.apache.nifi.processors.standard.PutSQL.json') as json_file:
#     data=json_file.read()
# jsonobj = json.loads(data)

# schema = {
#    "type": "object",
#         "properties" : {
#             "scheduledState":{"type": "string", "enum": ["ENABLED"]},
#             "properties":{
#                 "type" :"object",
#                 "properties" : {
#                     "JDBC Connection Pool":{"type": "string",
#                             "pattern": '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
#                             "minLength": 36,
#                             "maxLength": 36}
#                 },
#                 "required": ["JDBC Connection Pool"]
#             },
#         },
#         "required": ["scheduledState"]
# }
# v = Draft201909Validator(schema)
# validate(jsonobj, schema)
