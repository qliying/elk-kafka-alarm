from celery import Celery
from celery.schedules import crontab

app = Celery('tasks', broker='redis://127.0.0.1:6379/1')

# 定时任务必须和 tasks.py 里的函数名一致
app.conf.beat_schedule = {
    'traffic-monitor-task': {
        'task': 'tasks.monitor_nginx_traffic',
        'schedule': crontab(minute='*/5'),
    },
}

# 必须导入 tasks，让 Worker 能找到函数
import tasks
