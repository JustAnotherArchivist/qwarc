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
	def __init__(self, prefix, maxFileSize, dedupe, command, specFile, specDependencies, logFilename):
		'''
		Initialise the WARC writer

		prefix: str, path prefix for WARCs; a dash, a five-digit number, and ".warc.gz" will be appended.
		maxFileSize: int, maximum size of an individual WARC. Use 0 to disable splitting.
		dedupe: bool, whether to enable record deduplication
		command: list, the command line call for qwarc
		specFile: str, path to the spec file
		specDependencies: qwarc.utils.SpecDependencies
		logFilename: str, name of the log file written by this process
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

		self._logFilename = logFilename

		self._metaWarcinfoRecordID = None
		self._write_meta_warc(self._write_initial_meta_records)

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
		payload = io.BytesIO(json.dumps(data, indent = 2).encode('utf-8'))
		# Workaround for https://github.com/webrecorder/warcio/issues/87
		digester = warcio.utils.Digester('sha1')
		digester.update(payload.getvalue())
		record = self._warcWriter.create_warc_record(
		    None,
		    'warcinfo',
		    payload = payload,
		    warc_headers_dict = {'Content-Type': 'application/json; charset=utf-8', 'WARC-Block-Digest': str(digester)},
		    length = len(payload.getvalue()),
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
			r.rawRequestData.seek(0, io.SEEK_END)
			length = r.rawRequestData.tell()
			r.rawRequestData.seek(0)
			requestRecord = self._warcWriter.create_warc_record(
			    str(r.url),
			    'request',
			    payload = r.rawRequestData,
			    length = length,
			    warc_headers_dict = {
			      'WARC-Date': requestDate,
			      'WARC-IP-Address': r.remoteAddress[0],
			      'WARC-Warcinfo-ID': self._metaWarcinfoRecordID,
			    }
			  )
			requestRecordID = requestRecord.rec_headers.get_header('WARC-Record-ID')
			r.rawResponseData.seek(0, io.SEEK_END)
			length = r.rawResponseData.tell()
			r.rawResponseData.seek(0)
			responseRecord = self._warcWriter.create_warc_record(
			    str(r.url),
			    'response',
			    payload = r.rawResponseData,
			    length = length,
			    warc_headers_dict = {
			      'WARC-Date': requestDate,
			      'WARC-IP-Address': r.remoteAddress[0],
			      'WARC-Concurrent-To': requestRecordID,
			      'WARC-Warcinfo-ID': self._metaWarcinfoRecordID,
			    }
			  )
			payloadDigest = responseRecord.rec_headers.get_header('WARC-Payload-Digest')
			assert payloadDigest is not None
			if self._dedupe and responseRecord.payload_length > 100: # Don't deduplicate small responses; the additional headers are typically larger than the payload dedupe savings...
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
					      'WARC-Warcinfo-ID': self._metaWarcinfoRecordID,
					    }
					  )
					# Workaround for https://github.com/webrecorder/warcio/issues/94
					responseRecord.rec_headers.replace_header('WARC-Profile', 'http://netpreserve.org/warc/1.1/revisit/identical-payload-digest')
				else:
					self._dedupeMap[payloadDigest] = (responseRecord.rec_headers.get_header('WARC-Record-ID'), str(r.url), requestDate)
			self._warcWriter.write_record(requestRecord)
			self._warcWriter.write_record(responseRecord)

		if self._maxFileSize and self._file.tell() > self._maxFileSize:
			self._close_file()

	def _write_resource_records(self):
		'''Write spec file and dependencies'''
		assert self._metaWarcinfoRecordID is not None, 'write_warcinfo_record must be called first'

		for type_, contentType, fn in itertools.chain((('specfile', 'application/x-python', self._specFile),), map(lambda x: ('spec-dependency-file', 'application/octet-stream', x), self._specDependencies.files)):
			with open(fn, 'rb') as f:
				f.seek(0, io.SEEK_END)
				length = f.tell()
				f.seek(0)
				record = self._warcWriter.create_warc_record(
				    f'file://{fn}',
				    'resource',
				    payload = f,
				    length = length,
				    warc_headers_dict = {'X-QWARC-Type': type_, 'WARC-Warcinfo-ID': self._metaWarcinfoRecordID, 'Content-Type': contentType},
				  )
				self._warcWriter.write_record(record)

	def _write_initial_meta_records(self):
		self._metaWarcinfoRecordID = self._write_warcinfo_record()
		self._write_resource_records()

	def _write_log_record(self):
		assert self._metaWarcinfoRecordID is not None, 'write_warcinfo_record must be called first'

		rootLogger = logging.getLogger()
		for handler in rootLogger.handlers: #FIXME: Uses undocumented attribute handlers
			handler.flush()
		with open(self._logFilename, 'rb') as fp:
			fp.seek(0, io.SEEK_END)
			length = fp.tell()
			fp.seek(0)
			record = self._warcWriter.create_warc_record(
			    f'file://{self._logFilename}',
			    'resource',
			    payload = fp,
			    length = length,
			    warc_headers_dict = {'X-QWARC-Type': 'log', 'Content-Type': 'text/plain; charset=utf-8', 'WARC-Warcinfo-ID': self._metaWarcinfoRecordID},
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
		self._write_meta_warc(self._write_log_record)
