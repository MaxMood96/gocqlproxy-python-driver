from cassandra.cluster import DefaultConnection, Session
from cassandra.connection import locally_supported_compressions
from cassandra.protocol import ProxiedMessage


compressor, decompressor = locally_supported_compressions['snappy']


class ProxyConnection(DefaultConnection):

    compressor = staticmethod(compressor)
    decompressor = staticmethod(decompressor)

    def __init__(self, host='127.0.0.1', port=9042, *args, **kwargs):
        return super().__init__(host, port, *args, **kwargs)

    def _send_options_message(self):
        # options message is not necessary for proxy - it's handled by the
        # proxy <-> scylla connection.
        self.connected_event.set()

    def send_msg(self, msg, *args, **kwargs):
        if isinstance(msg, ProxiedMessage):
            original_msg = msg.message
        else:
            original_msg = msg
            msg = ProxiedMessage(msg, b'')
        if getattr(original_msg, 'consistency_level', None) is None:
            # ResponseFuture._set_result requires message.consistency_level
            # for ServerError messages. PrepareMessage for instance doesn't
            # have this attribute set, so it crashes with AttributeError
            original_msg.consistency_level = None
        return super().send_msg(msg, *args, **kwargs)


class ProxySession(Session):

    def __init__(self, cluster, hosts):
        cluster.connection_factory = ProxyConnection
        super().__init__(cluster, hosts)

    def _create_response_future(self, *args, **kwargs):
        future = super()._create_response_future(*args, **kwargs)
        default_routing_key = b''
        routing_key = future.query.routing_key or default_routing_key
        proxy_message = ProxiedMessage(future.message, routing_key)
        future.message = proxy_message
        return future
