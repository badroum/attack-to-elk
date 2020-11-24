from elasticsearch import Elasticsearch, helpers
import os
class ElasticSearch:
    def __init__(self, index):
        es_host     = os.environ['es_hostname']
        es_port     = os.environ['es_port']
        es_username     = os.environ['es_username']
        es_password     = os.environ['es_password']


        if es_host is None:
            exit('You need to export Elasticsearch hostname')
        if es_port is None:
            exit('You need to export Elasticsearch port number')

        self.es     = Elasticsearch([{'host': es_host, 'port': es_port}], http_auth=(es_username, es_password))

        self.index  = index


    def delete_index(self):
        if self.exists():
            self._result = self.es.indices.delete(self.index)
        return self
    
    def exists(self):
        return self.es.indices.exists(self.index)
    
    def create_index(self):
        self._result= self.es.indices.create(self.index)
        return self

    def add_bulk(self, data, vtype):
        actions = []
        for item in data:
            item_data = {
                            "_index" :  self.index,
                            "_type"  :  vtype,
                            "_source":  item,
                        }
            actions.append(item_data)
        return helpers.bulk(self.es, actions, index=self.index)
