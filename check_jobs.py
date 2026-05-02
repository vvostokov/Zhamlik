import sys
sys.path.insert(0, '/opt/zhamlik')

from app import create_app
app = create_app()

# Find scheduler directly
from extensions import scheduler
print(f"Scheduler: {scheduler}")

jobs = scheduler.get_jobs()
print(f"\nJobs count: {len(jobs)}")
for job in jobs:
    print(f"  - {job.id}: next={job.next_run_time}")

# Trigger debug job via scheduler
print("\n--- Running debug job via scheduler ---")
scheduler.run_job('debug_ping')