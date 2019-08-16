import fcntl
import gzip
import io
import itertools
import json
import logging
import os
import qwarc.utils
import tempfile
import time
import warcio


class WARC:
	def __init__(self, prefix, maxFileSize, dedupe, command, specFile, specDependencies):
		'''
		Initialise the WARC writer

		prefix: str, path prefix for WARCs; a dash, a five-digit number, and ".warc.gz" will be appended.
		maxFileSize: int, maximum size of an individual WARC. Use 0 to disable splitting.
		dedupe: bool, whether to enable record deduplication
		command: list, the command line call for qwarc
		specFile: str, path to the spec file
		specDependencies: qwarc.utils.SpecDependencies
		'''

		self._prefix = prefix
		self._counter = 0
		self._maxFileSize = maxFileSize

		self._closed = True
		self._file = None
		self._warcWriter = None

		self._dedupe = dedupe
		self._dedupeMap = {}

		self._command = command
		self._specFile = specFile
		self._specDependencies = specDependencies

		self._logFile = None
		self._logHandler = None
		self._setup_logger()

		self._dataWarcinfoRecordID = None
		self._metaWarcinfoRecordID = None
		self._write_meta_warc(self._write_initial_meta_records)

	def _setup_logger(self):
		rootLogger = logging.getLogger()
		formatter = qwarc.utils.LogFormatter()
		self._logFile = tempfile.NamedTemporaryFile(prefix = 'qwarc-warc-', suffix = '.log.gz', delete = False)
		self._logHandler = logging.StreamHandler(io.TextIOWrapper(gzip.GzipFile(filename = self._logFile.name, mode = 'wb'), encoding = 'utf-8'))
		self._logHandler.setFormatter(formatter)
		rootLogger.addHandler(self._logHandler)
		self._logHandler.setLevel(logging.INFO)

	def _ensure_opened(self):
		'''Open the next file that doesn't exist yet if there is currently no file opened'''

		if not self._closed:
			return
		while True:
			filename = f'{self._prefix}-{self._counter:05d}.warc.gz'
			try:
				# Try to open the file for writing, requiring that it does not exist yet, and attempt to get an exclusive, non-blocking lock on it
				self._file = open(filename, 'xb')
				fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
			except FileExistsError:
				logging.info(f'{filename} already exists, skipping')
				self._counter += 1
			else:
				break
		logging.info(f'Opened {filename}')
		self._warcWriter = warcio.warcwriter.WARCWriter(self._file, gzip = True, warc_version = '1.1')
		self._closed = False
		self._counter += 1
		self._dataWarcinfoRecordID = self._write_warcinfo_record()

	def _write_warcinfo_record(self):
		data = {
			'software': qwarc.utils.get_software_info(self._specFile, self._specDependencies),
			'command': self._command,
			'files': {
				'spec': self._specFile,
				'spec-dependencies': self._specDependencies.files
			  },
			'extra': self._specDependencies.extra,
		  }
		record = self._warcWriter.create_warc_record(
		    'urn:qwarc:warcinfo',
		    'warcinfo',
		    payload = io.BytesIO(json.dumps(data, indent = 2).encode('utf-8')),
		    warc_headers_dict = {'Content-Type': 'application/json; charset=utf-8'},
		  )
		self._warcWriter.write_record(record)
		return record.rec_headers.get_header('WARC-Record-ID')

	def write_client_response(self, response):
		'''
		Write the requests and responses stored in a ClientResponse instance to the currently opened WARC.
		A new WARC will be started automatically if the size of the current file exceeds the limit after writing all requests and responses from this `response` to the current WARC.
		'''

		self._ensure_opened()
		for r in response.iter_all():
			usec = f'{(r.rawRequestTimestamp - int(r.rawRequestTimestamp)):.6f}'[2:]
			requestDate = time.strftime(f'%Y-%m-%dT%H:%M:%S.{usec}Z', time.gmtime(r.rawRequestTimestamp))
			requestRecord = self._warcWriter.create_warc_record(
			    str(r.url),
			    'request',
			    payload = io.BytesIO(r.rawRequestData),
			    warc_headers_dict = {
			      'WARC-Date': requestDate,
			      'WARC-IP-Address': r.remoteAddress[0],
			      'WARC-Warcinfo-ID': self._dataWarcinfoRecordID,
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
			      'WARC-Warcinfo-ID': self._dataWarcinfoRecordID,
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
					      'WARC-Warcinfo-ID': self._dataWarcinfoRecordID,
					    }
					  )
				else:
					self._dedupeMap[payloadDigest] = (responseRecord.rec_headers.get_header('WARC-Record-ID'), str(r.url), requestDate)
			self._warcWriter.write_record(requestRecord)
			self._warcWriter.write_record(responseRecord)

		if self._maxFileSize and self._file.tell() > self._maxFileSize:
			self.close()

	def _write_resource_records(self):
		'''Write spec file and dependencies'''
		assert self._metaWarcinfoRecordID is not None, 'write_warcinfo_record must be called first'

		for type_, contentType, fn in itertools.chain((('specfile', 'application/x-python', self._specFile),), map(lambda x: ('spec-dependency-file', 'application/octet-stream', x), self._specDependencies.files)):
			with open(fn, 'rb') as f:
				record = self._warcWriter.create_warc_record(
				    f'file://{fn}',
				    'resource',
				    payload = f,
				    warc_headers_dict = {'X-QWARC-Type': type_, 'WARC-Warcinfo-ID': self._metaWarcinfoRecordID, 'Content-Type': contentType},
				  )
				self._warcWriter.write_record(record)

	def _write_initial_meta_records(self):
		self._metaWarcinfoRecordID = self._write_warcinfo_record()
		self._write_resource_records()

	def _write_log_record(self):
		assert self._metaWarcinfoRecordID is not None, 'write_warcinfo_record must be called first'

		self._logHandler.flush()
		self._logHandler.stream.close()
		record = self._warcWriter.create_warc_record(
		    'urn:qwarc:log',
		    'resource',
		    payload = gzip.GzipFile(self._logFile.name),
		    warc_headers_dict = {'Content-Type': 'text/plain; charset=utf-8', 'WARC-Warcinfo-ID': self._metaWarcinfoRecordID},
		  )
		self._warcWriter.write_record(record)

	def _close_file(self):
		'''Close the currently opened WARC'''

		if not self._closed:
			self._file.close()
			self._warcWriter = None
			self._file = None
			self._closed = True

	def _write_meta_warc(self, callback):
		filename = f'{self._prefix}-meta.warc.gz'
		#TODO: Handle OSError on fcntl.flock and retry
		self._file = open(filename, 'ab')
		try:
			fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
			logging.info(f'Opened {filename}')
			self._warcWriter = warcio.warcwriter.WARCWriter(self._file, gzip = True, warc_version = '1.1')
			self._closed = False

			callback()
		finally:
			self._close_file()

	def close(self):
		'''Clean up everything.'''
		self._close_file()
		logging.getLogger().removeHandler(self._logHandler)
		self._write_meta_warc(self._write_log_record)
		try:
			os.remove(self._logFile.name)
		except OSError:
			logging.error('Could not remove temporary log file')
		self._logFile = None
		self._logHandler.close()
		self._logHandler = None
