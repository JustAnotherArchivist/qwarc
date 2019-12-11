STATUS_TODO = 0
'''Status of an item that has not been processed yet'''

STATUS_INPROGRESS = 1
'''Status of an item that is currently being processed'''

STATUS_DONE = 2
'''Status of an item that has been processed'''

STATUS_ERROR = 3
'''Status of an item during whose processing an error occurred'''

ACTION_SUCCESS = 0
'''Treat this response as a success'''

ACTION_IGNORE = 1 #TODO Replace with ACTION_SUCCESS since it's really the same thing.
'''Ignore this response'''

ACTION_RETRY = 2
'''Retry the same request'''

ACTION_FOLLOW_OR_SUCCESS = 3
'''If the response contains a Location or URI header, follow it. Otherwise, treat it as a success.'''
#TODO: Rename to ACTION_FOLLOW maybe? However, the current name makes it more clear what qwarc does when there's a redirect without a redirect target...

ACTION_RETRIES_EXCEEDED = 4
'''This request failed repeatedly and exceeded the retry limit.'''
