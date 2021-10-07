import click
import json
import pandas as pd
from nifi_check_list.NifiMyltyGraph import NifiMultyGraph
import logging
from nifi_check_list.validate_nifi import getAllComponent, checkConsumeKafkaRecor, checkAllProcessorsIsEnables, \
    checkAllProcessorValidName, checkMergeContentBeforePut, checkSchemaObjects

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log = logging.getLogger("nifi_check_list") 

@click.command()
@click.option('--file',  help='Validate file must be a json struct')
@click.option(
    '--log',
    default="ERROR",
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
    g = NifiMultyGraph()
    g.nifiSchemaLoad(jsonobj)
    allComponent = getAllComponent(jsonobj)
    result_check = pd.DataFrame()
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
    report.to_csv('report.csv', sep='\t', index=False, quotechar='"')


if __name__ == "__main__":
    main()
