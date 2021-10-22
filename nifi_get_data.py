import pandas as pd
import yaml
import click
from nifi_check_list.validate_nifi import getAllComponent, checkConsumeKafkaRecor, checkAllProcessorsIsEnables, \
    checkAllProcessorValidName, checkMergeContentBeforePut, checkSchemaObjects, checkAllProcessorsIsNormal
from nifi_check_list.NifiMyltyGraph import NifiMultyGraph, ErrorLoadGraph
from nifi_check_list.utils import unload_error_json, unload_error_csv
from nifi_check_list.NifiInstance import NifiInstance, ErrorIdGroup, ErrorRegestry, AccessError
import logging
import urllib3
urllib3.disable_warnings()

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log = logging.getLogger("nifi_remote_check_list")


@click.command()
@click.option('--id', required=True, help='Инедтификатор процессонрной группы')
@click.option('--config', 'yaml_file', default='config_nifi.yml', help='Инедтификатор процессонрной группы')
@click.option(
    '--log',
    'logLevel',
    default="ERROR",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    help='Log level. Default log level DEBUG')
def main(id, yaml_file, logLevel):
    logging.basicConfig(
        level=logLevel,
        format=FORMAT
    )
    # id = 'f80e9187-fc45-3e54-9207-b124432f6a62'
    # yaml_file = 'config.yml'
    log.info("Попали сюда")
    log.info("Загружаю конфигурацию")
    try:
        with open(yaml_file, 'r') as ymlfile:
            config = yaml.load(ymlfile, yaml.SafeLoader)
        log.info("Передаю конфигарацию")
        log.info(config)
        n = NifiInstance(config)
    except AccessError as access:
        log.error(access)
        exit(1)
    except Exception as e:
        log.error(f'Проблемы с загрузкой конфигурации {e}')
        exit(1)
    try:
        group_info = n.get_process_groups_info(id)
    except ErrorRegestry as reg:
        unload_error_csv(id, reg.message)
    except ErrorIdGroup as group:
        unload_error_csv(id, group.message)
    result_check = pd.DataFrame()
    log.info("Подготовка сетевых тестов")
    check = checkAllProcessorsIsNormal(group_info)
    result_check = result_check.append(check)
    jsonobj = n.get_process_groups(id)
    try:
        g = NifiMultyGraph()
        g.nifiSchemaLoad(jsonobj)
    except ErrorLoadGraph as e:
        unload_error_json(id, jsonobj)
        unload_error_csv(id, e.message, mode='a')
        log.error(e)
        exit(1)
    # Локальные тесты
    allComponent = getAllComponent(jsonobj, id)
    log.debug(allComponent)
    check = checkAllProcessorsIsEnables(jsonobj)
    result_check = result_check.append(check)
    check = checkAllProcessorValidName(jsonobj)
    result_check = result_check.append(check)
    check = checkConsumeKafkaRecor(g)
    result_check = result_check.append(check)
    check = checkMergeContentBeforePut(g, jsonobj)
    result_check = result_check.append(check)
    check = checkSchemaObjects(jsonobj)
    result_check = result_check.append(check)
    report = allComponent.merge(result_check, how='inner', on='Identifier')
    report.to_csv(f'{id}.csv', sep='\t', index=False, quotechar='"')


if __name__ == "__main__":
    main()





# https://nifi.devdata.lmru.tech:443/nifi-api/flow/process-groups/4ff67677-016b-1000-2900-7f12f5340e17
# https://nifi.devdata.lmru.tech/nifi-api/process-groups/fc7734f6-571e-35df-bab9-1a094f696f27/download?access_token=hWS8BaJ2fR3EhVqCzs0zIKiSdQw-erLBQz2bXwSzCS0


# for item in test:
#             print (str(item .id))