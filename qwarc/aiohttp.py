import aiohttp
import aiohttp.client_proto
import aiohttp.connector
import functools
import io
import itertools
import qwarc.utils
import time
import tempfile


# aiohttp does not expose the raw data sent over the wire, so we need to get a bit creative...
# The ResponseHandler handles received data; the writes are done directly on the underlying transport.
# So ResponseHandler is replaced with a class which keeps all received data in a list, and the transport's write method is replaced with one which sends back all written data to the ResponseHandler.
# Because the ResponseHandler instance disappears when the connection is closed (ClientResponse.{_response_eof,close,release}), ClientResponse copies the references to the data objects in the RequestHandler.
# aiohttp also does connection pooling/reuse, so ClientRequest resets the raw data when the request is sent. (This would not work with pipelining, but aiohttp does not support pipelining: https://github.com/aio-libs/aiohttp/issues/1740 )
# This code has been developed for aiohttp version 2.3.10.


class RawData:
	def __init__(self):
		self.requestTimestamp = None
		self.requestData = tempfile.SpooledTemporaryFile(max_size = 1048576, dir = './')
		self.responseTimestamp = None
		self.responseData = tempfile.SpooledTemporaryFile(max_size = 1048576, dir = './')

	def close(self):
		self.requestData.close()
		self.responseData.close()


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
		self.rawData.responseData.seek(0, io.SEEK_END)
		self.rawData.responseData.write(data)

	def reset_raw_data(self):
		if self.rawData:
			self.rawData.close()
		self.rawData = RawData()


def make_transport_write(transport, protocol):
	transport._real_write = transport.write
	def write(self, data):
		if protocol.rawData.requestTimestamp is None:
			protocol.rawData.requestTimestamp = time.time()
		protocol.rawData.requestData.seek(0, io.SEEK_END)
		protocol.rawData.requestData.write(data)
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
		return qwarc.utils.ReadonlyFileView(self._rawData.requestData)

	@property
	def rawResponseTimestamp(self):
		return self._rawData.responseTimestamp

	@property
	def rawResponseData(self):
		return qwarc.utils.ReadonlyFileView(self._rawData.responseData)

	@property
	def remoteAddress(self):
		return self._remoteAddress

	def set_history(self, history):
		self._history = history #FIXME: Uses private attribute of aiohttp.client_reqrep.ClientResponse

	def iter_all(self):
		return itertools.chain(self.history, (self,))

	async def _read(self, nbytes = None):
		#FIXME: This uses internal undocumented APIs of aiohttp
		payload = Payload()
		self._rawData.responseData.seek(0)
		beginning = self._rawData.responseData.read(32768) # Headers must fit into 32 KiB. That's more than most clients out there, but aiohttp does *not* have this restriction!
		pos = beginning.find(b'\r\n\r\n')
		assert pos > -1, 'Could not find end of headers'
		respMsg = aiohttp.http_parser.HttpResponseParserPy().parse_message(beginning[:pos + 2].split(b'\r\n'))
		try:
			length = int(self.headers.get('Content-Length'))
		except (KeyError, ValueError, TypeError):
			length = None
		parser = aiohttp.http_parser.HttpPayloadParser(payload, length = length, chunked = respMsg.chunked, compression = respMsg.compression, code = respMsg.code, method = self.method)
		eof, data = parser.feed_data(beginning[pos + 4:])
		while True:
			chunk = self._rawData.responseData.read(1048576)
			if not chunk:
				break
			eof, data = parser.feed_data(chunk)
			if nbytes is not None and payload.data.tell() >= nbytes:
				if payload.exc:
					raise Exception from payload.exc
				return payload.data.getvalue()[:nbytes]
			# data can only not be None if eof is True, so there is no need to actually do anything about it
			if eof:
				break
		if not eof:
			parser.feed_eof()

		if payload.exc:
			raise Exception from payload.exc
		return payload.data.getvalue()

	async def read(self, nbytes = None):
		'''
		Read up to nbytes from the response payload, or the entire response if nbytes is None.
		Note that this method always starts from the beginning of the response even if called repeatedly.
		'''
		#FIXME: Uses internal aiohttp attribute _content
		if nbytes is not None:
			if self._content is not None:
				return self._content[:nbytes]
			return (await self._read(nbytes))
		if self._content is None:
			self._content = await self._read()
		return self._content

	async def release(self):
		if not self.closed:
			self.connection.reset_raw_data()
		await super().release()


class Payload:
	# A class implementing the minimal subset used by the HttpPayloadParser to retrieve the data
	def __init__(self):
		self.data = io.BytesIO()
		self.exc = None

	def feed_data(self, data, size):
		self.data.write(data)

	def feed_eof(self):
		pass

	def set_exception(self, exc):
		self.exc = exc

	def begin_http_chunk_receiving(self):
		pass

	def end_http_chunk_receiving(self):
		pass
