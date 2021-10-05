from networkx import MultiDiGraph
import jsonpath
import pandas as pd
import logging

log = logging.getLogger("NifiMultiGraph")


class NifiMultyGraph (MultiDiGraph):
    def nifiSchemaLoad(self, nifiSchema):
        log.debug('Инициализиреум процессоры как вершины')
        processors = jsonpath.jsonpath(nifiSchema, '$..processors.*')
        for item in processors:
            self.add_node(
                item['identifier'],
                type=item['type'],
                name=item['name']
            )
        log.debug('Инициализиреум связи как ребра')
        connections = jsonpath.jsonpath(nifiSchema, '$.flowContents.connections.*')
        for item in connections:
            for rel in item['selectedRelationships']:
                self.add_edge(
                    item['source']['id'],
                    item['destination']['id'],
                    type=rel,
                    weight=1
                )

    def selectNodeWithAttribute(self, attribute_type, attribute_value) -> list:
        nodes = [x for x, y in self.nodes(data=True) if y[attribute_type] == attribute_value]
        return nodes

    def checkConsumeKafkaRecord(self) -> pd.DataFrame:
        log.debug('Начинаем поиск на графе ConsumeKafkaRecord и выход на ConvertRecord по parse.failure')
        log.debug('Выбираем все процессоры ConsumeKafkaRecord')
        nodesKafka = self.selectNodeWithAttribute(
            attribute_type='type',
            attribute_value='org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_0'
        )
        result = pd.DataFrame()
        log.debug('Обходим кажный процессор ConsumeKafkaRecord')
        for item in nodesKafka:
            log.debug(f'Процессор с номером = {item}')
            temp_result = pd.DataFrame(
                [[
                    item,
                    0
                ]],
                columns=['identifier', 'ConsumeKafkaRecord_2_0'],
                index=['identifier']
            )
            for n, nbdict in self.adj[item].items():
                log.debug(f'Ищем связь по parse.failure для item = {item}')
                log.debug(nbdict)
                log.debug(self.nodes[n].get('type'))
                for idEdge, typeEdge in nbdict.items():
                    if (
                        typeEdge.get('type') == 'parse.failure'
                        and self.nodes[n].get('type') == 'org.apache.nifi.processors.standard.ConvertRecord'
                    ):
                        temp_result = pd.DataFrame()
                        log.debug(f'Нашли нормальную связь parse.failure для   {item} -> {n}')
                        break
                    else:
                        log.debug(f'Не та связь для {item} -> {n}')
                result = result.append(temp_result)
        log.debug('Удаляем дубликаты проверки')
        result = result.drop_duplicates()
        return result
