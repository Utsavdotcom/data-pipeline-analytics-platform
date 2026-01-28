# data-pipeline-analytics-platform
 
## Batch Transformation Scheduling

The raw-to-analytics transformation is automated using Windows Task Scheduler.

The scheduled task runs the following command:

.venv\Scripts\python.exe -m scripts.transform_raw_to_analytics

The task is configured to:
- run every 15 minutes
- use the project virtual environment
- write execution logs to `scheduler.log`

This setup ensures analytics tables are updated automatically without manual intervention.
