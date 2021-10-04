import yaml
import nipyapi

ca_file = client_cert_file = client_key_file = None
token = None
yaml_file = 'C:/Users/60067287/PycharmProjects/nifi_nipyapi/config.yml'
ca_file = 'C:/Users/60067287/PycharmProjects/nifi_nipyapi/nifi_cert.pem'

pg_name_for_change = 'test_api_ktkachev'
pg_id_for_change = '104e3a1a-d352-1370-8c27-f7135cf90f58'
processor_name_for_change = 'PutEmail_for_TEST_api'
processor_id_for_change = '7557361f-bb83-1e94-914b-5e7b4a37b05a'
variables_update_list = [('kafka.system.Debezium','tpnet'),('kafka.system.GoldenGate','rms')]
property_update_dict = {'SMTP Hostname':'owa.leroymerlin.ru'}

def nifi_get_variables(**parameters) -> bool:
    try:
        result = False
        if parameters:
            vars = nipyapi.canvas.get_variable_registry(parameters['process_group'])
            my_vars = vars.variable_registry.variables
            print('Process group variables: ')
            for var in my_vars:
                print('name: ', var.variable.name,', value: ',var.variable.value)
            print('------------------------------------------------------------------------------------------')
            result = True
            return result
    except Exception as e:
        print('Update variable exception: ', e)
        raise Exception('Update variable exception: ', e)

def nifi_api_usage(config):
    try:
        nipyapi.security.set_service_ssl_context(service='nifi', ca_file=ca_file, client_cert_file=None, client_key_file=None, client_key_password=None)
        '''
        client = nipyapi.nifi.apis.access_api.AccessApi()
        token = client.create_access_token(username=config['credentials']['login'], password=config['credentials']['password'])
        '''
        if nipyapi.security.service_login(service='nifi', username=config['credentials']['login'], password=config['credentials']['password'], bool_response=True):
            print('NiFi version: ',nipyapi.system.get_nifi_version_info())
            print('------------------------------------------------------------------------------------------')
            #print('NiFi root process group: ',nipyapi.canvas.get_root_pg_id())
            # 1. Processor groups and variables
            # 1.1. Get processor group
            # by name
            #pg = nipyapi.canvas.get_process_group(pg_name_for_change, identifier_type='name')
            # by id
            pg = nipyapi.canvas.get_process_group(pg_id_for_change, identifier_type='id')
            pg = pg[0] if isinstance(pg, list) else pg
            print('Process group id: ', pg.id)
            print('------------------------------------------------------------------------------------------')
            # 1.2. Stop  processor group
            nipyapi.canvas.schedule_process_group(pg.id, False)
            # 1.3. Get processor group variables
            nifi_get_variables(process_group=pg)
            # 1.4. Update variable
            nipyapi.canvas.update_variable_registry(pg, variables_update_list, refresh=True)
            # 1.5. Get processor group updated variables
            nifi_get_variables(process_group=pg)
            # 1.6. Start  processor group
            nipyapi.canvas.schedule_process_group(pg.id, True)
            print('------------------------------------------------------------------------------------------')
            ###################################################################################################
            ###################################################################################################
            ###################################################################################################
            # 2. Processor and processor properties
            # 2.1. Get processor
            # by name
            #test_processor = nipyapi.canvas.get_processor(processor_name_for_change,identifier_type='name')
            # by id
            test_processor = nipyapi.canvas.get_processor(processor_id_for_change, identifier_type='id')
            print(test_processor.id)
            print(test_processor.component.config.properties)
            # 2.2. Stop processor
            nipyapi.canvas.schedule_processor(test_processor, False, refresh=True)
            processor_config = test_processor.component.config
            #nipyapi.nifi.models.processor_config_dto.ProcessorConfigDTO()
            if isinstance(processor_config, nipyapi.nifi.ProcessorConfigDTO):
                # 2.3. Change processor properties
                processor_config.properties.update(property_update_dict)
                nipyapi.canvas.update_processor(test_processor, processor_config)
                # 2.4. Start processor
                nipyapi.canvas.schedule_processor(test_processor, True, refresh=True)
            nipyapi.security.service_logout(service='nifi')
    except Exception as e:
        print('NiFi API usage Exception: ',e)
        raise Exception('NiFi API usage Exception: ', e)

if __name__ == '__main__':
    try:
        with open(yaml_file, 'r') as ymlfile:
            config = yaml.load(ymlfile, yaml.SafeLoader)
        nipyapi.config.nifi_config.host = config['connections']['nifi_url']
        # nipyapi.config.registry_config.host = 'http://localhost:18080/nifi-registry-api'
        nifi_api_usage(config)
    except Exception as e:
        print('Main procedure Exception: ',e)
        raise Exception('Main procedure Exception: ', e)