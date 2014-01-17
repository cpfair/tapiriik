def strip_context(exc):
	exc.__context__ = exc.__cause__ = exc.__traceback__ = None
	return exc