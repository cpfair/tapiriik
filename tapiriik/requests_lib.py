# For whatever reason there's no built-in way to specify a global timeout for requests operations.
# socket.setdefaulttimeout doesn't work since requests overriddes the default with its own default.

def patch_requests_with_default_timeout(timeout):
	import requests
	old_request = requests.Session.request
	def new_request(*args, **kwargs):
		if "timeout" not in kwargs:
			kwargs["timeout"] = timeout
		return old_request(*args, **kwargs)
	requests.Session.request = new_request

def patch_requests_no_verify_ssl():
	import requests
	old_request = requests.Session.request
	def new_request(*args, **kwargs):
		kwargs.update({"verify": False})
		return old_request(*args, **kwargs)
	requests.Session.request = new_request

# Not really patching requests here, but...
def patch_requests_source_address(new_source_address):
	import socket
	old_create_connection = socket.create_connection
	def new_create_connection(address, timeout=None, source_address=None):
		if address[1] in [80, 443]:
			return old_create_connection(address, timeout, new_source_address)
		else:
			return old_create_connection(address, timeout, source_address)
	socket.create_connection = new_create_connection
