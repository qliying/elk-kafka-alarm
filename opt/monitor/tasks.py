from celery_app import app
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from kafka import KafkaConsumer
import psutil
import socket
import time

# ==================== 原有配置 完全保留 ====================
KAFKA_BROKERS = ['kafka1:9092','kafka2:9092','kafka3:9092']
KAFKA_TOPIC = 'nginx-log'
KAFKA_GROUP_ID = 'traffic-monitor'
MAIL_FROM = "3308346492@qq.com"
MAIL_TO = ["3308346492@qq.com"]
MAIL_SMTP = "smtp.qq.com"
MAIL_PORT = 465
MAIL_AUTH = "brlgydnykkvbdbai"
# ==============================================

# -------------------------- 1. 系统资源监控（CPU/内存/磁盘） --------------------------
def get_system_stats():
    cpu_usage = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    mem_usage = mem.percent
    mem_available = round(mem.available / 1024 / 1024 / 1024, 2)
    disk = psutil.disk_usage('/')
    disk_usage = disk.percent
    disk_available = round(disk.free / 1024 / 1024 / 1024, 2)
    return cpu_usage, mem_usage, mem_available, disk_usage, disk_available

# -------------------------- 2. 网卡流量监控（10秒平均速率，解决0流量问题） --------------------------
def get_network_stats(interface="eth0"):
    # 第一次采样
    net_io1 = psutil.net_io_counters(pernic=True)[interface]
    # 等待10秒，计算这段时间的平均流量
    time.sleep(10)
    # 第二次采样
    net_io2 = psutil.net_io_counters(pernic=True)[interface]
    
    # 计算10秒内的总流量，换算成MB/s
    rx_bytes = net_io2.bytes_recv - net_io1.bytes_recv
    tx_bytes = net_io2.bytes_sent - net_io1.bytes_sent
    rx_mb = round(rx_bytes / 1024 / 1024 / 10, 2)  # 除以10秒，得到平均MB/s
    tx_mb = round(tx_bytes / 1024 / 1024 / 10, 2)
    return rx_mb, tx_mb

# -------------------------- 3. Kafka日志分析 + 错误日志前5条 --------------------------
def get_nginx_stats():
    total = 0
    status_200 = 0
    status_404 = 0
    status_5xx = 0
    error_logs = []  # 存错误日志

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000
    )

    for msg in consumer:
        log = msg.value.decode("utf-8")
        total += 1
        parts = log.split()
        if len(parts) > 8:
            status = parts[8]
            if status == "200":
                status_200 += 1
            elif status == "404":
                status_404 += 1
                error_logs.append(log)
            elif status in ("500","502","503"):
                status_5xx += 1
                error_logs.append(log)

    # 取前5条错误日志，换行拼接
    top5_errors = "<br>".join(error_logs[:5]) if error_logs else "无错误日志"
    return total, status_200, status_404, status_5xx, top5_errors

# -------------------------- Celery定时主任务 --------------------------
@app.task
def monitor_nginx_traffic():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 执行所有监控
    cpu_usage, mem_usage, mem_available, disk_usage, disk_available = get_system_stats()
    rx_mb, tx_mb = get_network_stats()
    total, status_200, status_404, status_5xx, top5_errors = get_nginx_stats()

    # 邮件内容
    html = f"""
    <html>
    <body style="font-family:微软雅黑;">
    <h2>📊 服务器监控 + Nginx日志分析报表</h2>
    <p>监控时间：{now}</p>
    <hr>

    <h3>一、系统资源状态</h3>
    <p>CPU 使用率：{cpu_usage}%</p>
    <p>内存 使用率：{mem_usage}%（可用：{mem_available}GB）</p>
    <p>磁盘 使用率：{disk_usage}%（可用：{disk_available}GB）</p>
    <hr>

    <h3>二、网卡流量监控（eth0，10秒平均）</h3>
    <p>入站平均流量：{rx_mb} MB/s</p>
    <p>出站平均流量：{tx_mb} MB/s</p>
    <hr>

    <h3>三、Nginx日志统计</h3>
    <p>总请求数：{total}</p>
    <p>正常请求(200)：{status_200}</p>
    <p>404错误：{status_404}</p>
    <p>5xx服务错误：{status_5xx}</p>
    <hr>

    <h3>四、前5条错误日志</h3>
    <p>{top5_errors}</p>
    </body>
    </html>
    """

    msg = MIMEText(html, 'html', 'utf-8')
    msg["Subject"] = "【服务器监控】系统+流量+日志分析报表"
    msg["From"] = MAIL_FROM
    msg["To"] = ",".join(MAIL_TO)

    try:
        server = smtplib.SMTP_SSL(MAIL_SMTP, MAIL_PORT)
        server.login(MAIL_FROM, MAIL_AUTH)
        server.sendmail(MAIL_FROM, MAIL_TO, msg.as_string())
        server.quit()
        print(f"[{now}] 监控报表发送成功")
    except Exception as e:
        print(f"[{now}] 发送失败：{e}")
