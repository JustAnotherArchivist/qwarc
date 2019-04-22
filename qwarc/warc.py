import fcntl
import io
import logging
import time
import warcio


class WARC:
	def __init__(self, prefix, maxFileSize, dedupe):
		'''
		Initialise the WARC writer

		prefix: str, path prefix for WARCs; a dash, a five-digit number, and ".warc.gz" will be appended.
		maxFileSize: int, maximum size of an individual WARC. Use 0 to disable splitting.
		dedupe: bool, whether to enable record deduplication
		'''

		self._prefix = prefix
		self._counter = 0
		self._maxFileSize = maxFileSize

		self._closed = True
		self._file = None
		self._warcWriter = None

		self._dedupe = dedupe
		self._dedupeMap = {}

		self._cycle()

	def _cycle(self):
		'''Close the current file, open the next file that doesn't exist yet'''

		#TODO: This opens a new file also at the end, which can result in empty WARCs. Should try to reorder this to only open a WARC when writing a record, and to only close the current WARC if the size is exceeded after write_client_response.
		self.close()
		while True:
			filename = '{}-{:05d}.warc.gz'.format(self._prefix, self._counter)
			try:
				# Try to open the file for writing, requiring that it does not exist yet, and attempt to get an exclusive, non-blocking lock on it
				self._file = open(filename, 'xb')
				fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
			except FileExistsError:
				logging.info('{} already exists, skipping'.format(filename))
				self._counter += 1
			else:
				break
		logging.info('Opened {}'.format(filename))
		self._warcWriter = warcio.warcwriter.WARCWriter(self._file, gzip = True)
		self._closed = False
		self._counter += 1

	def write_client_response(self, response):
		'''
		Write the requests and responses stored in a ClientResponse instance to the currently opened WARC.
		A new WARC will be started automatically if the size of the current file exceeds the limit after writing all requests and responses from this `response` to the current WARC.
		'''

		for r in response.iter_all():
			requestDate = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(r.rawRequestTimestamp))
			requestRecord = self._warcWriter.create_warc_record(
			    str(r.url),
			    'request',
			    payload = io.BytesIO(r.rawRequestData),
			    warc_headers_dict = {
			      'WARC-Date': requestDate,
			      'WARC-IP-Address': r.remoteAddress[0],
			    }
			  )
			requestRecordID = requestRecord.rec_headers.get_header('WARC-Record-ID')
			responseRecord = self._warcWriter.create_warc_record(
			    str(r.url),
			    'response',
			    payload = io.BytesIO(r.rawResponseData),
			    warc_headers_dict = {
			      'WARC-Date': requestDate,
			      'WARC-IP-Address': r.remoteAddress[0],
			      'WARC-Concurrent-To': requestRecordID,
			    }
			  )
			payloadDigest = responseRecord.rec_headers.get_header('WARC-Payload-Digest')
			assert payloadDigest is not None
			if self._dedupe and responseRecord.payload_length > 0: # Don't "deduplicate" empty responses
				if payloadDigest in self._dedupeMap:
					refersToRecordId, refersToUri, refersToDate = self._dedupeMap[payloadDigest]
					responseHttpHeaders = responseRecord.http_headers
					responseRecord = self._warcWriter.create_revisit_record(
					    str(r.url),
					    digest = payloadDigest,
					    refers_to_uri = refersToUri,
					    refers_to_date = refersToDate,
					    http_headers = responseHttpHeaders,
					    warc_headers_dict = {
					      'WARC-Date': requestDate,
					      'WARC-IP-Address': r.remoteAddress[0],
					      'WARC-Concurrent-To': requestRecordID,
					      'WARC-Refers-To': refersToRecordId,
					      'WARC-Truncated': 'length',
					    }
					  )
				else:
					self._dedupeMap[payloadDigest] = (responseRecord.rec_headers.get_header('WARC-Record-ID'), str(r.url), requestDate)
			self._warcWriter.write_record(requestRecord)
			self._warcWriter.write_record(responseRecord)

		if self._maxFileSize and self._file.tell() > self._maxFileSize:
			self._cycle()

	def close(self):
		'''Close the currently opened WARC'''

		if not self._closed:
			self._file.close()
			self._warcWriter = None
			self._file = None
			self._closed = True
