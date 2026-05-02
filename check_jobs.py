import sys
sys.path.insert(0, '/opt/zhamlik')

from datetime import datetime
import time

# Wait a bit for job to run
print("Waiting 40 seconds for debug job to execute...")
time.sleep(40)

# Check if file exists
import os
if os.path.exists("/tmp/scheduler_ping.txt"):
    print("File exists!")
    with open("/tmp/scheduler_ping.txt") as f:
        print(f.read())
else:
    print("File does NOT exist - job not executed")