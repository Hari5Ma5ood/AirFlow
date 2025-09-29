from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch


class ElasticHook(BaseHook):

    def __init__(self, conn_id='elastic', *args, **kwargs):

        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id)

        con_config = {}
        hosts = []

        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            con_config['port'] = conn.port
        if conn.login:
            con_config['http_auth'] = (conn.login, conn.password)

        self.es = Elasticsearch(hosts, **con_config)
        self.index = conn.schema

    def info(self):
        return self.es.info()

    def set_index(self, index):
        self.index = index
    def add_doc(self, index, doc_type, doc):
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res

class AirflowElasticPlugin(AirflowPlugin):
    name = 'elastic'
    hooks = [ElasticHook]