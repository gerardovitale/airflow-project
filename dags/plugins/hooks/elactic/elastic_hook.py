from typing import Any

from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch


class ElasticHook(BaseHook):

    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn = self.get_connection(conn_id)

        conn_config = {}
        hosts = []

        if self._conn.host:
            hosts = self._conn.host.split(',')
        if self._conn.port:
            conn_config['port'] = int(self._conn.port)
        if self._conn.login:
            conn_config['http_auth'] = (self._conn.login, self._conn.password)

        self.es = Elasticsearch(hosts, **conn_config)
        self.index = self._conn.schema

    def get_conn(self) -> Any:
        return self._conn

    def info(self):
        return self.es.info()

    def set_index(self, index):
        self.index = index

    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res
