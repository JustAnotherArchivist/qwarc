import aiohttp
import aiohttp.client_proto
import aiohttp.connector
import functools
import itertools
import time


# aiohttp does not expose the raw data sent over the wire, so we need to get a bit creative...
# The ResponseHandler handles received data; the writes are done directly on the underlying transport.
# So ResponseHandler is replaced with a class which keeps all received data in a list, and the transport's write method is replaced with one which sends back all written data to the ResponseHandler.
# Because the ResponseHandler instance disappears when the connection is closed (ClientResponse.{_response_eof,close,release}), ClientResponse copies the references to the data objects in the RequestHandler.
# aiohttp also does connection pooling/reuse, so ClientRequest resets the raw data when the request is sent. (This would not work with pipelining, but aiohttp does not support pipelining: https://github.com/aio-libs/aiohttp/issues/1740 )
# This code has been developed for aiohttp version 2.3.10.

#TODO: THERE IS A MEMORY LEAK HERE SOMEWHERE! I spent a whole day trying to find it without success.


class RawData:
	def __init__(self):
		self.requestTimestamp = None
		self.requestData = []
		self.responseTimestamp = None
		self.responseData = []


class ResponseHandler(aiohttp.client_proto.ResponseHandler):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.rawData = None
		self.remoteAddress = None

	def data_received(self, data):
		super().data_received(data)
		if not data:
			return
		if self.rawData.responseTimestamp is None:
			self.rawData.responseTimestamp = time.time()
		self.rawData.responseData.append(data)

	def reset_raw_data(self):
		self.rawData = RawData()


def make_transport_write(transport, protocol):
	transport._real_write = transport.write
	def write(self, data):
		if protocol.rawData.requestTimestamp is None:
			protocol.rawData.requestTimestamp = time.time()
		protocol.rawData.requestData.append(data)
		self._real_write(data)
	return write


class TCPConnector(aiohttp.connector.TCPConnector):
	def __init__(self, *args, loop = None, **kwargs):
		super().__init__(*args, loop = loop, **kwargs)
		self._factory = functools.partial(ResponseHandler, loop = loop)

	async def _wrap_create_connection(self, protocolFactory, host, port, *args, **kwargs): #FIXME: Uses internal API
		transport, protocol = await super()._wrap_create_connection(protocolFactory, host, port, *args, **kwargs)
		transport.write = make_transport_write(transport, protocol).__get__(transport, type(transport)) # https://stackoverflow.com/a/28127947
		protocol.remoteAddress = (host, port)
		return (transport, protocol)


class ClientRequest(aiohttp.client_reqrep.ClientRequest):
	def send(self, connection):
		connection.protocol.reset_raw_data()
		return super().send(connection)


class ClientResponse(aiohttp.client_reqrep.ClientResponse):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._rawData = None
		self._remoteAddress = None

	async def start(self, connection, readUntilEof):
		self._rawData = connection.protocol.rawData
		self._remoteAddress = connection.protocol.remoteAddress
		return (await super().start(connection, readUntilEof))

	@property
	def rawRequestTimestamp(self):
		return self._rawData.requestTimestamp

	@property
	def rawRequestData(self):
		return b''.join(self._rawData.requestData)

	@property
	def rawResponseTimestamp(self):
		return self._rawData.responseTimestamp

	@property
	def rawResponseData(self):
		return b''.join(self._rawData.responseData)

	@property
	def remoteAddress(self):
		return self._remoteAddress

	def set_history(self, history):
		self._history = history #FIXME: Uses private attribute of aiohttp.client_reqrep.ClientResponse

	def iter_all(self):
		return itertools.chain(self.history, (self,))

	async def release(self):
		if not self.closed:
			self.connection.reset_raw_data()
		await super().release()
