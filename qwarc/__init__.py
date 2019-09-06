import qwarc.aiohttp
from qwarc.const import *
import qwarc.utils
import qwarc.warc


import aiohttp as _aiohttp
if _aiohttp.__version__ != '2.3.10':
	raise ImportError('aiohttp must be version 2.3.10')
import asyncio
import collections
import concurrent.futures
import itertools
import logging
import os
import random
import sqlite3
import yarl


class Item:
	itemType = None

	def __init__(self, itemValue, session, headers, warc):
		self.itemValue = itemValue
		self.session = session
		self.headers = headers
		self.warc = warc
		self.stats = {'tx': 0, 'rx': 0, 'requests': 0}
		self.logger = logging.LoggerAdapter(logging.getLogger(), {'itemType': self.itemType, 'itemValue': self.itemValue})

		self.childItems = []

	async def fetch(self, url, responseHandler = qwarc.utils.handle_response_default, method = 'GET', data = None, headers = [], verify_ssl = True):
		'''
		HTTP GET or POST a URL

		url: str or yarl.URL
		responseHandler: a callable that determines how the response is handled. See qwarc.utils.handle_response_default for details.
		method: str, must be 'GET' or 'POST'
		data: dict or list/tuple of lists/tuples of length two or bytes or file-like or None, the data to be sent in the request body
		headers: list of 2-tuples, additional headers for this request only

		Returns response (a ClientResponse object or None) and history (a tuple of (response, exception) tuples).
			response can be None and history can be an empty tuple, depending on the circumstances (e.g. timeouts).
		'''

		#TODO: Rewrite using 'async with self.session.get'

		url = yarl.URL(url) # Explicitly convert for normalisation, percent-encoding, etc.
		assert method in ('GET', 'POST'), 'method must be GET or POST'
		headers = self.headers + headers
		#TODO Deduplicate headers with later values overriding earlier ones
		history = []
		attempt = 0
		#TODO redirectLevel
		while True:
			attempt += 1
			response = None
			exc = None
			action = ACTION_RETRY
			writeToWarc = True
			try:
				try:
					with _aiohttp.Timeout(60):
						self.logger.info(f'Fetching {url}')
						response = await self.session.request(method, url, data = data, headers = headers, allow_redirects = False, verify_ssl = verify_ssl)
						try:
							ret = await response.read()
						except:
							# No calling the handleResponse callback here because this is really bad. The not-so-bad exceptions (e.g. an error during reading the response) will be caught further down.
							response.close()
							raise
						else:
							tx = len(response.rawRequestData)
							rx = len(response.rawResponseData)
							self.logger.info(f'Fetched {url}: {response.status} (tx {tx}, rx {rx})')
							self.stats['tx'] += tx
							self.stats['rx'] += rx
							self.stats['requests'] += 1
				except (asyncio.TimeoutError, _aiohttp.ClientError) as e:
					self.logger.error(f'Request for {url} failed: {e!r}')
					action, writeToWarc = await responseHandler(url, attempt, response, e)
					exc = e # Pass the exception outward for the history
				else:
					action, writeToWarc = await responseHandler(url, attempt, response, None)
				history.append((response, exc))
				if action in (ACTION_SUCCESS, ACTION_IGNORE):
					return response, tuple(history)
				elif action == ACTION_FOLLOW_OR_SUCCESS:
					redirectUrl = response.headers.get('Location') or response.headers.get('URI')
					if not redirectUrl:
						return response, tuple(history)
					url = url.join(yarl.URL(redirectUrl))
					if response.status in (301, 302, 303) and method == 'POST':
						method = 'GET'
						data = None
					attempt = 0
				elif action == ACTION_RETRIES_EXCEEDED:
					self.logger.error(f'Request for {url} failed {attempt} times')
					return response, tuple(history)
				elif action == ACTION_RETRY:
					# Nothing to do, just go to the next cycle
					pass
			finally:
				if response:
					if writeToWarc:
						self.warc.write_client_response(response)
					await response.release()

	async def process(self):
		raise NotImplementedError

	@classmethod
	def generate(cls):
		yield from () # Generate no items by default

	@classmethod
	def _gen(cls):
		for x in cls.generate():
			yield (cls.itemType, x, STATUS_TODO)

	def add_item(self, itemClassOrType, itemValue):
		if issubclass(itemClassOrType, Item):
			item = (itemClassOrType.itemType, itemValue)
		else:
			item = (itemClassOrType, itemValue)
		if item not in self.childItems:
			self.childItems.append(item)


class QWARC:
	def __init__(self, itemClasses, warcBasePath, dbPath, command, specFile, specDependencies, logFilename, concurrency = 1, memoryLimit = 0, minFreeDisk = 0, warcSizeLimit = 0, warcDedupe = False):
		'''
		itemClasses: iterable of Item
		warcBasePath: str, base name of the WARC files
		dbPath: str, path to the sqlite3 database file
		command: list, the command line used to invoke qwarc
		specFile: str, path to the spec file
		specDependencies: qwarc.utils.SpecDependencies
		logFilename: str, name of the log file written by this process
		concurrency: int, number of concurrently processed items
		memoryLimit: int, gracefully stop when the process uses more than memoryLimit bytes of RSS; 0 disables the memory check
		minFreeDisk: int, pause when there's less than minFreeDisk space on the partition where WARCs are written; 0 disables the disk space check
		warcSizeLimit: int, size of each WARC file; 0 if the WARCs should not be split
		'''

		self._itemClasses = itemClasses
		self._itemTypeMap = {cls.itemType: cls for cls in itemClasses}
		self._warcBasePath = warcBasePath
		self._dbPath = dbPath
		self._command = command
		self._specFile = specFile
		self._specDependencies = specDependencies
		self._logFilename = logFilename
		self._concurrency = concurrency
		self._memoryLimit = memoryLimit
		self._minFreeDisk = minFreeDisk
		self._warcSizeLimit = warcSizeLimit
		self._warcDedupe = warcDedupe

		self._reset_working_vars()

	def _reset_working_vars(self):
		# Working variables
		self._db = None
		self._tasks = set()
		self._sleepTasks = set()
		self._sessions = [] # aiohttp.ClientSession instances
		self._freeSessions = collections.deque() # ClientSession instances that are currently free
		self._warc = None

	async def obtain_exclusive_db_lock(self):
		c = self._db.cursor()
		while True:
			try:
				c.execute('BEGIN EXCLUSIVE')
				break
			except sqlite3.OperationalError as e:
				if str(e) != 'database is locked':
					raise
				await asyncio.sleep(1)
		return c

	def _make_item(self, itemType, itemValue, session, headers):
		try:
			itemClass = self._itemTypeMap[itemType]
		except KeyError:
			raise RuntimeError(f'No such item type: {itemType!r}')
		return itemClass(itemValue, session, headers, self._warc)

	async def _wait_for_free_task(self):
		if not self._tasks:
			return
		done, pending = await asyncio.wait(self._tasks, return_when = concurrent.futures.FIRST_COMPLETED)
		for future in done:
			# TODO Replace all of this with `if future.cancelled():`
			try:
				await future #TODO: Is this actually necessary? asyncio.wait only returns 'done' futures...
			except concurrent.futures.CancelledError as e:
				# Got cancelled, nothing we can do about it, but let's log a warning if it's a process task
				if isinstance(future, asyncio.Task):
					if future.taskType == 'process_item':
						logging.warning(f'Task for {future.itemType}:{future.itemValue} cancelled: {future!r}')
					elif future.taskType == 'sleep':
						self._sleepTasks.remove(future)
				continue
			if future.taskType == 'sleep':
				# Dummy task for empty todo list, see below.
				self._sleepTasks.remove(future)
				continue
			item = future.item
			logging.info(f'{future.itemType}:{future.itemValue} done: {item.stats["requests"]} requests, {item.stats["tx"]} tx, {item.stats["rx"]} rx')
			cursor = await self.obtain_exclusive_db_lock()
			try:
				cursor.execute('UPDATE items SET status = ? WHERE id = ?', (STATUS_DONE, future.id))
				cursor.execute('COMMIT')
			except:
				cursor.execute('ROLLBACK')
				raise
			await self._insert_subitems(item)
			self._freeSessions.append(item.session)
		self._tasks = pending

	async def _insert_subitems(self, item):
		cursor = await self.obtain_exclusive_db_lock()
		try:
			if item.childItems:
				it = iter(item.childItems)
				while True:
					values = [(t, v, STATUS_TODO) for t, v in itertools.islice(it, 100000)]
					if not values:
						break
					cursor.executemany('INSERT OR IGNORE INTO items (type, value, status) VALUES (?, ?, ?)', values)
			cursor.execute('COMMIT')
		except:
			cursor.execute('ROLLBACK')
			raise

	async def run(self, loop):
		headers = [('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0')] #TODO: Move elsewhere

		for i in range(self._concurrency):
			session = _aiohttp.ClientSession(
			  connector = qwarc.aiohttp.TCPConnector(loop = loop),
			  request_class = qwarc.aiohttp.ClientRequest,
			  response_class = qwarc.aiohttp.ClientResponse,
			  skip_auto_headers = ['Accept-Encoding'],
			  loop = loop
			)
			self._sessions.append(session)
			self._freeSessions.append(session)

		self._warc = qwarc.warc.WARC(self._warcBasePath, self._warcSizeLimit, self._warcDedupe, self._command, self._specFile, self._specDependencies, self._logFilename)

		self._db = sqlite3.connect(self._dbPath, timeout = 1)
		self._db.isolation_level = None # Transactions are handled manually below.
		self._db.execute('PRAGMA synchronous = OFF')

		try:
			while True:
				while len(self._tasks) >= self._concurrency:
					await self._wait_for_free_task()

				if self._minFreeDisk and qwarc.utils.too_little_disk_space(self._minFreeDisk):
					logging.info('Disk space is low, sleeping')
					sleepTask = asyncio.ensure_future(asyncio.sleep(random.uniform(self._concurrency / 2, self._concurrency * 1.5)))
					sleepTask.taskType = 'sleep'
					self._tasks.add(sleepTask)
					self._sleepTasks.add(sleepTask)
					continue

				if os.path.exists('STOP'):
					logging.info('Gracefully shutting down due to STOP file')
					break
				if self._memoryLimit and qwarc.utils.uses_too_much_memory(self._memoryLimit):
					logging.info(f'Gracefully shutting down due to memory usage (current = {qwarc.utils.get_rss()} > limit = {self._memoryLimit})')
					break

				cursor = await self.obtain_exclusive_db_lock()
				try:
					cursor.execute('SELECT id, type, value, status FROM items WHERE status = ? LIMIT 1', (STATUS_TODO,))
					result = cursor.fetchone()
					if not result:
						if cursor.execute('SELECT id, status FROM items WHERE status != ? LIMIT 1', (STATUS_DONE,)).fetchone():
							# There is currently no item to do, but there are still some in progress, so more TODOs may appear in the future.
							# It would be nice if we could just await wait_for_free_task() here, but that doesn't work because those TODOs might be in another process.
							# So instead, we insert a dummy task which just sleeps a bit. Average sleep time is equal to concurrency, i.e. one check per second.
							#TODO: The average sleep time is too large if there are only few sleep tasks; scale with len(sleepTasks)/self._concurrency?
							sleepTask = asyncio.ensure_future(asyncio.sleep(random.uniform(self._concurrency / 2, self._concurrency * 1.5)))
							sleepTask.taskType = 'sleep'
							self._tasks.add(sleepTask)
							self._sleepTasks.add(sleepTask)
							cursor.execute('COMMIT')
							continue
						else:
							# Really nothing to do anymore
							#TODO: Another process may be running create_db, in which case we'd still want to wait...
							# create_db could insert a dummy item which is marked as done when the DB is ready
							cursor.execute('COMMIT')
							break
					id, itemType, itemValue, status = result
					cursor.execute('UPDATE items SET status = ? WHERE id = ?', (STATUS_INPROGRESS, id))
					cursor.execute('COMMIT')
				except:
					cursor.execute('ROLLBACK')
					raise

				session = self._freeSessions.popleft()
				item = self._make_item(itemType, itemValue, session, headers)
				task = asyncio.ensure_future(item.process())
				#TODO: Is there a better way to add custom information to a task/coroutine object?
				task.taskType = 'process'
				task.id = id
				task.itemType = itemType
				task.itemValue = itemValue
				task.item = item
				self._tasks.add(task)

			for sleepTask in self._sleepTasks:
				sleepTask.cancel()

			while len(self._tasks):
				await self._wait_for_free_task()

			logging.info('Done')
		except (Exception, KeyboardInterrupt) as e:
			# Kill all tasks
			for task in self._tasks:
				task.cancel()
			await asyncio.wait(self._tasks, return_when = concurrent.futures.ALL_COMPLETED)

			raise
		finally:
			for session in self._sessions:
				session.close()
			self._warc.close()
			self._db.close()

			self._reset_working_vars()

	def create_db(self):
		db = sqlite3.connect(self._dbPath, timeout = 1)
		db.execute('PRAGMA synchronous = OFF')
		with db:
			db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, type TEXT, value TEXT, status INTEGER)')
			db.execute('CREATE INDEX items_status_idx ON items (status)')
			db.execute('CREATE UNIQUE INDEX items_type_value_idx ON items (type, value)')

		it = itertools.chain(*(i._gen() for i in self._itemClasses))
		while True:
			values = tuple(itertools.islice(it, 100000))
			if not values:
				break
			with db:
				db.executemany('INSERT INTO items (type, value, status) VALUES (?, ?, ?)', values)
