import tapiriik.database
tapiriik.database.db = tapiriik.database._connection["tapiriik_test"]
tapiriik.database.cachedb = tapiriik.database._connection["tapiriik_cache_test"]

from tapiriik.testing import *
import unittest
unittest.main()

tapiriik.database._connection.drop_database("tapiriik_test")
tapiriik.database._connection.drop_database("tapiriik_cache_test")
