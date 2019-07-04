from cassandra.cluster import DefaultConnection, Session
from cassandra.connection import locally_supported_compressions
from cassandra.protocol import ProxiedMessage


compressor, decompressor = locally_supported_compressions['snappy']


class ProxyConnection(DefaultConnection):

    compressor = staticmethod(compressor)
    decompressor = staticmethod(decompressor)

    def _send_options_message(self):
        # options message is not necessary for proxy - it's handled by the
        # proxy <-> scylla connection.
        self.connected_event.set()

    def send_msg(self, msg, *args, **kwargs):
        if not isinstance(msg, ProxiedMessage):
            msg = ProxiedMessage(msg, b'')
        return super().send_msg(msg, *args, **kwargs)


class ProxySession(Session):

    def __init__(self, cluster, hosts):
        cluster.connection_factory = ProxyConnection
        super().__init__(cluster, hosts)

    def _create_response_future(self, *args, **kwargs):
        future = super()._create_response_future(*args, **kwargs)
        routing_key = future.query.routing_key or b''
        proxy_message = ProxiedMessage(future.message, routing_key)
        future.message = proxy_message
        return future
