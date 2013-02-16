from tapiriik.sync import Sync
import time
import datetime
print("Sync worker starting at " + datetime.datetime.now().ctime())
while True:
    Sync.PerformGlobalSync()
    time.sleep(5)
