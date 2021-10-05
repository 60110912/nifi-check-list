# jsonpath_lagou.py

import os
import re
import jsonpath
import json
from jsonschema import validate, exceptions  # ,  Draft201909Validator
from validation_shcema import nifiValidationShcemas
import pandas as pd
from NifiMyltyGraph import NifiMultyGraph
import logging
from logging import config

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.DEBUG,
    format=FORMAT)



def allProcessorsIsEnables(jsonobj):
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    for idx, item in enumerate(processors):
        try:
            validate(item, nifiValidationShcemas['processors_enabled'])
            print("Record #{}: OK\n".format(item['name']))
        except exceptions.ValidationError as ve:
            print(ve)
            print("Record #{}: ERROR\n".format(item['name']))


def allProcessorValidName(jsonobj):
    name_pattern = jsonpath.jsonpath(jsonobj, '$.flowContents.name')[0]
    name_pattern = '^' + name_pattern
    processors = jsonpath.jsonpath(jsonobj, '$..processors.*')
    for item in processors:
        if re.match(name_pattern, item['name']):
            print("Record #{}: OK\n".format(item['name']))
        else:
            print("Record #{}: ERROR\n".format(item['name']))


# Преобразовать строку формата json в объект python
def getAllComponent(jsonobj) -> pd.DataFrame:
    all_node = jsonobj['flowContents']
    result = pd.DataFrame(
                [[
                    all_node['identifier'],
                    all_node['name'],
                    all_node['componentType']
                ]],
                columns=['identifier', 'name', 'componentType'], 
                index=['identifier']
            )
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
    return result


if __name__ == "__main__":
    with open('outlook_exchange.json') as json_file:
        data = json_file.read()
    jsonobj = json.loads(data)
    #print(getAllComponent(jsonobj))
    #allProcessorsIsEnables(jsonobj)
    #allProcessorValidName(jsonobj)
    g = NifiMultyGraph()
    g.nifiSchemaLoad(jsonobj)
    test = g.checkConsumeKafkaRecord()
    print(test)


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