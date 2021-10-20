import click
import requests
from requests.auth import HTTPBasicAuth
import yaml
import logging


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log = logging.getLogger("get_config") 

@click.command()
@click.option('--url', required=True, help='Url to nifi regestry')
@click.option('--user', required=True, help='user name')
@click.option('--password', required=True, help='user password')
@click.option(
    '--log',
    'logLevel',
    default="ERROR",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    help='Log level. Default log level DEBUG')
def main(url, user, password, logLevel):
    logging.basicConfig(
        level=logLevel,
        format=FORMAT
    )
    path_access_token = 'access/token/login'
    log.debug(f'{url}/{path_access_token}')
    answer = requests.post(
                url=f'{url}/{path_access_token}',
                verify=False,
                auth=HTTPBasicAuth(username=user, password=password)
            )
    config = {}
    log.debug(f"код ответа от сервера {answer.status_code}")
    if answer.status_code == 201:
        config = {
            "nifi": {
                "registry_config":
                {
                    "host": url,
                    "access_token": answer.content
                }
            }
        }
    log.debug(config)
    with open('nifi_config.yaml', 'w') as file:
        yaml.dump(config, file)


if __name__ == "__main__":
    main()
