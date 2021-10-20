from requests.models import Response
import pandas as pd
from urllib3 import exceptions
import yaml
import json
from jsonschema import validate, Draft201909Validator
import requests
from nifi_check_list.utils import utf8
from nifi_check_list.encrypt_password import decryptSecret
from dataclasses import dataclass
from dacite import from_dict
from nifi_check_list.validate_nifi import getAllComponent, checkConsumeKafkaRecor, checkAllProcessorsIsEnables, \
    checkAllProcessorValidName, checkMergeContentBeforePut, checkSchemaObjects
from nifi_check_list.NifiMyltyGraph import NifiMultyGraph
from nifi_check_list.utils import unload_error_json
import logging
import urllib3
urllib3.disable_warnings()

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log = logging.getLogger("nifi_remote_check_list")


@dataclass
class ConfigNifi:
    """
    Объект описывающий конфигурацию Nifi
    """
    host: str
    username: str
    access_token: str
    password: str
    nifi_api: str
    regestry_api: str
    nifi_web: str
    regestry_api: str
    nifi_web: str


class AccessError(Exception):
    def __init__(self, message, errors):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        # Now for your custom code...
        self.errors = errors


class ErrorIdGroup(Exception):
    def __init__(self, message, errors):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        # Now for your custom code...
        self.errors = errors


class NifiInstance:
    """ The NifiInstance class facilitating easy to use
    methods utilizing the NiPyApi (https://github.com/Chaffelson/nipyapi)
    wrapper library.
    Arguments:
        url         (str): Nifi host url, defaults to environment variable `NIFI_HOST`.
        username    (str): Nifi username, defaults to environment variable `NIFI_USERNAME`.
        password    (str): Nifi password, defaults to environment variable `NIFI_PASSWORD`.
        verify_ssl  (bool): Whether to verify SSL connection - UNUSED as of now.
    """
    schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "username": {"type": "string"},
            "encrypPassword": {"type": "string"},
            "nifi_api": {"type": "string"},
            "regestry_api": {"type": "string"},
            "nifi_web": {"type": "string"}
            },
        "required": [
            "host", "username",
            "encrypPassword", "nifi_api", "regestry_api", "nifi_web"
        ]
    }

    def __init__(self, configDict):
        validate(configDict['nifi_config'], self.schema)
        self.config = configDict['nifi_config']
        self.config['password'] = decryptSecret(self.config['encrypPassword'])
        self.config['access_token'] = self._authenticate()
        self.configNifi = from_dict(data_class=ConfigNifi, data=self.config)

    def _authenticate(self) -> str:
        resorce_url = self.config["nifi_api"] + '/access/token'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        }

        data = {
            'username': self.config['username'],
            'password': self.config['password']
        }
        response = requests.post(url=self.config['host'] + resorce_url, headers=headers, data=data, verify=False)
        print(response.status_code)
        if response.status_code == 201:
            return utf8(response.content)
        else:
            raise AccessError('Не могу получить доступ Nifi')

    def makeAuthHeaders(self):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            "Authorization": 'Bearer {}'.format(self.configNifi.access_token)
        }
        return headers

    def get_process_groups(self, id='root') -> dict:
        """
        Функция возвращает процессорную группу
        Параметры:
            id - идентификатор группы
        """
        resorce_url = self.configNifi.nifi_api + f'/process-groups/{id}/download'
        response = requests.get(
            url=self.configNifi.host + resorce_url,
            headers=self.makeAuthHeaders(),
            verify=False
        )
        log.debug(response.status_code)
        if response.status_code == 200:
            return response.json()
        else:
            log.error("Нет такой процессорной группы")
            raise ErrorIdGroup("Нет такой процессорной группы")

    def get_process_groups_info(self, id='root') -> dict:
        """
        Функция возвращает информацию по процессорной группе
        Параметры:
            id - идентификатор группы
        """
        resorce_url = self.configNifi.nifi_api + f'/process-groups/{id}'
        response = requests.get(
            url=self.configNifi.host + resorce_url,
            headers=self.makeAuthHeaders(),
            verify=False
        )
        log.debug(response.status_code)
        if response.status_code == 200:
            return response.json()
        else:
            log.error("Нет такой процессорной группы")
            raise ErrorIdGroup("Нет такой процессорной группы")


def main():
    logging.basicConfig(
        level='DEBUG',
        format=FORMAT
    )
    id = 'fc4da6a3-0176-1000-ffff-ffffe236d670'
    yaml_file = 'config.yml'
    log.debug("Попали сюда")
    log.debug("Загружаю конфигурацию")
    try:
        with open(yaml_file, 'r') as ymlfile:
            config = yaml.load(ymlfile, yaml.SafeLoader)
        log.debug("Передаю конфигарацию")
        log.debug(config)
        n = NifiInstance(config)
    except Exception as e:
        log.error('Main procedure Exception: ', e)
        raise Exception('Main procedure Exception: ', e)
    # valid=266c3a9f-e7a4-1a13-932d-b6672b6a4ecf 71903623-00fe-1936-8e08-a222380ebb41
    jsonobj = n.get_process_groups(id)

    try:
        g = NifiMultyGraph()
        g.nifiSchemaLoad(jsonobj)
    except Exception as e:
        unload_error_json(id, jsonobj)
        log.error(e)
        exit(1)
    # Локальные тесты
    # allComponent = getAllComponent(jsonobj)
    # result_check = pd.DataFrame()
    # check = checkAllProcessorsIsEnables(jsonobj)
    # result_check = result_check.append(check)
    # check = checkAllProcessorValidName(jsonobj)
    # result_check = result_check.append(check)
    # check = checkConsumeKafkaRecor(g)
    # result_check = result_check.append(check)
    # check = checkMergeContentBeforePut(g, jsonobj)
    # result_check = result_check.append(check)
    # check = checkSchemaObjects(jsonobj)
    # result_check = result_check.append(check)
    # report = allComponent.merge(result_check, how='inner', on='Identifier')
    # report.to_csv('report.csv', sep='\t', index=False, quotechar='"')
    group_info = n.get_process_groups_info(id)
    unload_error_json(id, group_info)


if __name__ == "__main__":
    main()





# https://nifi.devdata.lmru.tech:443/nifi-api/flow/process-groups/4ff67677-016b-1000-2900-7f12f5340e17
# https://nifi.devdata.lmru.tech/nifi-api/process-groups/fc7734f6-571e-35df-bab9-1a094f696f27/download?access_token=hWS8BaJ2fR3EhVqCzs0zIKiSdQw-erLBQz2bXwSzCS0


# for item in test:
#             print (str(item .id))