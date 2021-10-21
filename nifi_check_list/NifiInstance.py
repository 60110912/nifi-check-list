import jsonpath
from jsonschema import validate
from jsonschema.exceptions import ValidationError
import requests
from nifi_check_list.utils import utf8
from nifi_check_list.encrypt_password import decryptSecret
from dataclasses import dataclass
from dacite import from_dict
from nifi_check_list.validation_shcema import nifiValidationShcemas
import logging
import urllib3
urllib3.disable_warnings()

log = logging.getLogger("nifi_instance")


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
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        self.message = message


class ErrorIdGroup(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        self.message = message


class ErrorRegestry(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        self.message = message


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
        log.info(response.status_code)
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
        log.info(response.status_code)
        if response.status_code == 200:
            self._scheck_regestry_status(response.json())
            return response.json()
        else:
            log.error("Нет такой процессорной группы")
            raise ErrorIdGroup(f"Нет такой процессорной группы {id}")

    def _scheck_regestry_status(self, jsonobj: dict):
        testName = 'Обнаружение фиксации процессорной группы в регестри'
        log.info(f'Запускаем тест "{testName}"')
        log.debug(f'{jsonobj}')
        resource = jsonpath.jsonpath(jsonobj, '$..versionControlInformation')
        if isinstance(resource, bool):
            log.info(f'Тест пройден "{testName}"')
            return
        log.info(f"Объект валидации {resource}")
        try:
            validate(resource, nifiValidationShcemas['versionControlInformation'])
        except ValidationError as ve:
            log.error(f"key={ve} Error")
            raise ErrorRegestry(f'Объект имеет изменения не зафиксированный в регестри.\n{ve}')