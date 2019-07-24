from qwarc.const import *
import aiohttp
import asyncio
import os


PAGESIZE = os.sysconf('SC_PAGE_SIZE')


def get_rss():
	'''Get the current RSS of this process in bytes'''

	with open('/proc/self/statm', 'r') as fp:
		return int(fp.readline().split()[1]) * PAGESIZE


def get_disk_free():
	'''Get the current free disk space on the relevant partition in bytes'''

	st = os.statvfs('.')
	return st.f_bavail * st.f_frsize


def uses_too_much_memory(limit):
	'''
	Check whether the process is using too much memory

	For performance reasons, this actually only checks the memory usage on every 100th call.
	'''

	uses_too_much_memory.callCounter += 1
	# Only check every hundredth call
	if uses_too_much_memory.callCounter % 100 == 0 and get_rss() > limit:
		return True
	return False
uses_too_much_memory.callCounter = 0


def too_little_disk_space(limit):
	'''
	Check whether the disk space is too small

	For performance reasons, this actually only checks the free disk space on every 100th call.
	'''

	too_little_disk_space.callCounter += 1
	if too_little_disk_space.callCounter % 100 == 0:
		too_little_disk_space.currentResult = (get_disk_free() < limit)
	return too_little_disk_space.currentResult
too_little_disk_space.callCounter = 0
too_little_disk_space.currentResult = False


# https://stackoverflow.com/a/4665027
def find_all(aStr, sub):
	'''Generator yielding the start positions of every non-overlapping occurrence of sub in aStr.'''

	start = 0
	while True:
		start = aStr.find(sub, start)
		if start == -1:
			return
		yield start
		start += len(sub)


def str_get_between(aStr, a, b):
	'''Get the string after the first occurrence of a in aStr and the first occurrence of b after that of a, or None if there is no such string.'''

	aPos = aStr.find(a)
	if aPos == -1:
		return None
	offset = aPos + len(a)
	bPos = aStr.find(b, offset)
	if bPos == -1:
		return None
	return aStr[offset:bPos]


def maybe_str_get_between(x, a, b):
	'''Like str_get_between, but returns None if x evaluates to False and converts it to a str before matching.'''

	if x:
		return str_get_between(str(x), a, b)


def str_get_all_between(aStr, a, b):
	'''Generator yielding every string between occurrences of a in aStr and the following occurrence of b.'''

	#TODO: This produces half-overlapping matches: str_get_all_between('aabc', 'a', 'c') will yield 'ab' and 'b'.
		# Might need to implement sending an offset to the find_all generator to work around this, or discard aOffset values which are smaller than the previous bPos+len(b).

	for aOffset in find_all(aStr, a):
		offset = aOffset + len(a)
		bPos = aStr.find(b, offset)
		if bPos != -1:
			yield aStr[offset:bPos]


def maybe_str_get_all_between(x, a, b):
	'''Like str_get_all_between, but yields no elements if x evaluates to False and converts x to a str before matching.'''

	if x:
		yield from str_get_all_between(str(x), a, b)


def generate_range_items(start, stop, step):
	'''
	Generator for items of `step` size between `start` and `stop` (inclusive)
	Yields strings of the form `'a-b'` where `a` and `b` are integers such that `b - a + 1 == step`, `min(a) == start`, and `max(b) == stop`.
	`b - a + 1` may be unequal to `step` on the last item if `(stop - start + 1) % step != 0` (see examples below).
	Note that `a` and `b` can be equal on the last item if `(stop - start) % step == 0` (see examples below).

	Examples:
	- generate_range_items(0, 99, 10) yields '0-9', '10-19', '20-29', ..., '90-99'
	- generate_range_items(0, 42, 10): '0-9', '10-19', '20-29', '30-39', '40-42'
	- generate_range_items(0, 20, 10): '0-9', '10-19', '20-20'
	'''

	for i in range(start, stop + 1, step):
		yield f'{i}-{min(i + step - 1, stop)}'


async def handle_response_default(url, attempt, response, exc):
	'''
	The default response handler, which behaves as follows:
	- If there is no response (e.g. timeout error), retry the retrieval after a delay of 5 seconds.
	- If the response has any of the status codes 401, 403, 404, 405, or 410, treat it as a permanent error and return.
	- If there was any exception and it is a asyncio.TimeoutError or a aiohttp.ClientError, treat as a potentially temporary error and retry the retrieval after a delay of 5 seconds.
	- If the response has any of the status codes 200, 204, 206, or 304, treat it as a success and return.
	- If the response has any of the status codes 301, 302, 303, 307, or 308, follow the redirect target if specified or return otherwise.
	- Otherwise, treat as a potentially temporary error and retry the retrieval after a delay of 5 seconds.

	- All responses are written to WARC by default.

	Note that this handler does not limit the number of retries on errors.

	Parameters: url (yarl.URL instance), attempt (int), response (aiohttp.ClientResponse or None), exc (Exception or None)
		At least one of response and exc is not None.
	Returns: (one of the qwarc.RESPONSE_* constants, bool signifying whether to write to WARC or not)
	'''

	#TODO: Document that `attempt` is reset on redirects

	if response is None:
		await asyncio.sleep(5)
		return ACTION_RETRY, True
	if response.status in (401, 403, 404, 405, 410):
		return ACTION_IGNORE, True
	if exc is not None and isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientError)):
		await asyncio.sleep(5)
		return ACTION_RETRY, True
	if response.status in (200, 204, 206, 304):
		return ACTION_SUCCESS, True
	if response.status in (301, 302, 303, 307, 308):
		return ACTION_FOLLOW_OR_SUCCESS, True
	await asyncio.sleep(5)
	return ACTION_RETRY, True


async def handle_response_ignore_redirects(url, attempt, response, exc):
	'''A response handler that does not follow redirects, i.e. treats them as a success instead. It behaves as handle_response_default otherwise.'''

	action, writeToWarc = await handle_response_default(url, attempt, response, exc)
	if action == ACTION_FOLLOW_OR_SUCCESS:
		action = ACTION_SUCCESS
	return action, writeToWarc


def handle_response_limit_error_retries(maxRetries, handler = handle_response_default):
	'''A response handler that limits the number of retries on errors. It behaves as handler otherwise, which defaults to handle_response_default.

	Technically, this is actually a response handler factory. This is so that the intuitive use works: fetch(..., responseHandler = handle_response_limit_error_retries(5))

	If you use the same limit many times, you should keep the return value (the response handler) of this method and reuse it to avoid creating a new function every time.
	'''

	async def _handler(url, attempt, response, exc):
		action, writeToWarc = await handler(url, attempt, response, exc)
		if action == ACTION_RETRY and attempt > maxRetries:
			action = ACTION_RETRIES_EXCEEDED
		return action, writeToWarc
	return _handler
