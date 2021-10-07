from networkx import MultiDiGraph, dijkstra_path_length
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
        log.debug('Инициализация графа закончена')

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

    def selectMergeContentBeforePut(self) -> str:
        log.debug('Начинаем поиск ближайщего MergeContent к GP или S3')
        log.debug('Выбираем все процессоры MergeContent')
        nodesMergeContent = self.selectNodeWithAttribute(
            attribute_type='type',
            attribute_value='org.apache.nifi.processors.standard.MergeContent'
        )
        result = None
        if len(nodesMergeContent) == 0:
            return result
        else:
            log.debug('Выбираем все процессоры PutS3Object')
            nodesPutObject = self.selectNodeWithAttribute(
                attribute_type='type',
                attribute_value='org.apache.nifi.processors.aws.s3.PutS3Object'
            )
            log.debug('Выбираем все процессоры PutSQL')
            nodesPutObject += self.selectNodeWithAttribute(
                attribute_type='type',
                attribute_value='org.apache.nifi.processors.standard.PutSQL'
            )
            if len(nodesPutObject) > 1:
                log.warning('Есть несколько вершин стока')
            elif len(nodesPutObject) == 0:
                log.warning('Нет вершин для стока')
                return result
            log.debug('Выбираем минимальное расстояние до Put стока')
            log.debug('Инициализируем первой вершиной из списка')
            resultMergeVertex = nodesMergeContent[0]
            minPath = float('inf')
            log.debug('Начинаем поиск вершины')
            for source in nodesMergeContent:
                for target in nodesPutObject:
                    diPathLen = dijkstra_path_length(self, source, target, weight='weight')
                    log.debug(f'Нашли расстояние межу вершинами {source} -> {target} len = {diPathLen}')
                    if diPathLen < minPath:
                        log.debug(f'Это расстояние пока минимально {source} -> {target} len = {diPathLen}')
                        minPath = diPathLen
                        resultMergeVertex = source
        return resultMergeVertex
