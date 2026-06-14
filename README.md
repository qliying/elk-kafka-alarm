# 企业级日志收集与监控平台搭建实施详细方案
## 一、展示效果如下
1. 这是一份 Nginx日志与服务器状态监控报表邮件，是搭建的日志告警平台生成的： 能够定时汇总了服务器的 CPU/内存/磁盘使用率、网卡流量，以及 Nginx 的请求统计；同时附上了最近的错误日志详情，方便我快速排查业务异常； 作为告警邮件，能不用登录服务器，就能实时掌握业务运行状态，实现问题早发现、早处理。

<!-- 这是一张图片，ocr 内容为： -->
![](https://cdn.nlark.com/yuque/0/2026/png/64435120/1781446955172-feb58dde-31ac-4a0e-a90d-759c0b48841e.png?x-oss-process=image%2Fformat%2Cwebp)

2.  这是基于 ELK 搭建的 Nginx日志可视化监控仪表盘，通过Kibana将日志数据转化为多维度图表，解决了传统日志难分析、问题定位慢的痛点，可通过以下链接实时查看业务运行状态。[http://47.97.68.105:5601/app/dashboards#/view/f96e80e0-67e0-11f1-9fa3-454618ca64f7?_g=(filters:!(),query:(language:kuery,query:''),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now))](http://47.97.68.105:5601/app/dashboards#/view/f96e80e0-67e0-11f1-9fa3-454618ca64f7?_g=(filters:!(),query:(language:kuery,query:''),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now)))

        仪表盘包含4个核心模块：

 ● 请求量趋势图：按时间维度统计请求量，直观反映流量变化；

  	● 状态码分布饼图：展示200/300/400状态码占比，快速识别业务健康度；

 	● 错误请求TOP10路径：定位高频错误路径，高效排查问题；

  	● 后端节点请求分布饼图：验证负载均衡分发效果，确保流量均匀分担。

<!-- 这是一张图片，ocr 内容为： -->
![](https://cdn.nlark.com/yuque/0/2026/png/64435120/1781447172457-3900f26b-cb14-41ce-8c94-dd7a6b5e1757.png?x-oss-process=image%2Fformat%2Cwebp)

## <font style="background-color:#CEF5F7;">二、本项目系统整体架构介绍如下：</font>
```nginx
elk-kafka-alarm/          # 项目根目录（与GitHub仓库名一致）
├── README.md             # 项目说明文档，记录架构、部署流程与使用说明
├── .gitignore            # Git忽略配置，排除Python缓存、日志文件、临时文件等
├── opt/                  # 对应服务器 /opt/ 目录，存放业务程序与代码
│   └── monitor/          # 告警服务代码根目录（部署在node02的/opt/monitor/）
│       ├── app.py        # Flask主程序，用于验证告警服务节点状态
│       ├── celery_app.py # Celery配置文件，定义定时告警任务与Redis连接
│       ├── tasks.py      # 日志解析、异常检测与邮件告警核心逻辑
│       └── test_monitor.sh # 自动化测试脚本，验证告警功能与定时任务
├── etc/                  # 对应服务器 /etc/ 目录，存放系统与组件配置文件（可选）
│   ├── nginx/            # Nginx相关配置
│   │   └── nginx.conf    # Nginx负载均衡配置，含日志格式与后端服务定义
│   ├── filebeat/         # Filebeat日志采集配置
│   │   └── filebeat.yml # 定义日志采集路径、Kafka输出等核心参数
│   ├── logstash/         # Logstash日志解析配置
│   │   └── conf.d/
│   │       └── nginx_log_pipeline.conf # 日志解析管道，含Grok过滤、ES输出
│   ├── elasticsearch/    # Elasticsearch存储配置
│   │   └── elasticsearch.yml # 集群名称、网络配置、JVM优化参数
│   └── kibana/           # Kibana可视化配置
│       └── kibana.yml    # ES连接地址、服务端口与主机绑定配置
└── usr/                  # 对应服务器 /usr/ 目录，存放系统服务配置（可选）
    └── lib/
        └── systemd/
            └── system/
                ├── kafka.service      # Kafka系统服务文件，定义启动/停止命令与依赖
                ├── logstash.service   # Logstash系统服务文件，配置用户权限与运行参数
                ├── elasticsearch.service # Elasticsearch系统服务文件，优化资源限制
                └── kibana.service     # Kibana系统服务文件，配置服务自启与重启策略
```

**项目名称**：基于 Filebeat+Kafka+ELK+Celery 的企业级 Nginx 日志收集、分析、可视化与告警平台  
**架构拓扑**：  
Nginx(负载均衡) → Filebeat(日志采集) → Kafka(消息队列) → Logstash(日志解析) → Elasticsearch(数据存储) → Kibana(可视化仪表盘)→Celery+Redis(日志检测+邮件告警)  
**服务器集群规划**（共3节点）

| 节点名称 | 内网IP | 角色分工 | 部署服 |
| --- | --- | --- | --- |
| node01(kafka1) | 172.19.65.58 | 入口/存储/可视化 | Nginx(负载均衡)、Filebeat、Elasticsearch、Kibana、Kafka |
| node02(kafka2) | 172.19.65.56 | 日志处理/告警 | 后端Web服务、Logstash、Python、Redis、Celery、Kafka |
| node03(kafka3) | 172.19.65.57 | 后端服务/集群节点 | 后端Web服务、Kafka |


> 系统环境：CentOS 7 ，全程使用 root 用户操作，所有命令可直接复制执行
>

---

## 三、三台节点通用操作：基础环境初始化
### 1. 关闭防火墙与 SELinux（所有节点执行）
生产环境可按需放行端口，测试环境直接关闭安全策略

```bash
# 临时关闭防火墙
systemctl stop firewalld
# 开机禁止自启防火墙
systemctl disable firewalld

# 临时关闭SELinux
setenforce 0
# 永久关闭SELinux
sed -i 's/SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config
```

### 2. 安装系统基础依赖（所有节点执行）
```bash
yum install -y epel-release
yum install -y wget vim net-tools java-11-openjdk.x86_64
# 校验Java环境（后续Kafka/ELK依赖）
java -version
```

---

## 四、节点专属配置：主机名与 Hosts 解析（分别执行）
保证集群内主机名互通，避免域名解析异常

### 1. node01（kafka1）执行
```bash
# 设置主机名
hostnamectl set-hostname kafka1
# 配置集群hosts映射
cat >> /etc/hosts << EOF
172.19.65.58 kafka1
172.19.65.56 kafka2
172.19.65.57 kafka3
EOF
```

### 2. node02（kafka2）执行
```bash
hostnamectl set-hostname kafka2
cat >> /etc/hosts << EOF
172.19.65.58 kafka1
172.19.65.56 kafka2
172.19.65.57 kafka3
EOF
```

### 3. node03（kafka3）执行
```bash
hostnamectl set-hostname kafka3
cat >> /etc/hosts << EOF
172.19.65.58 kafka1
172.19.65.56 kafka2
172.19.65.57 kafka3
EOF
```

---

## 五、全节点部署：Kafka 3节点集群
### 1. 下载并解压 Kafka（三台节点全部执行）
```bash
cd /opt
wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -zxvf kafka_2.13-3.6.1.tgz
cd kafka_2.13-3.6.1
```

### 2. 分别修改集群配置文件 `config/kraft/server.properties`
#### node01(kafka1) 配置
```bash
vim config/kraft/server.properties
```

写入/修改以下核心参数：

```properties
node.id=1
controller.quorum.voters=1@172.19.65.58:9093,2@172.19.65.56:9093,3@172.19.65.57:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka1:9092
```

#### node02(kafka2) 配置
```bash
vim config/kraft/server.properties
```

```properties
node.id=2
controller.quorum.voters=1@172.19.65.58:9093,2@172.19.65.56:9093,3@172.19.65.57:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka2:9092
```

#### node03(kafka3) 配置
```bash
vim config/kraft/server.properties
```

```properties
node.id=3
controller.quorum.voters=1@172.19.65.58:9093,2@172.19.65.56:9093,3@172.19.65.57:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka3:9092
```

### 3. 集群存储格式化（全节点执行）
1. **仅在 node01 生成集群 UUID**

```bash
/opt/kafka_2.13-3.6.1/bin/kafka-storage.sh random-uuid
```

复制输出的 UUID（示例：`abcdef123456`）

2. **三台节点统一执行格式化**（替换为你实际生成的UUID）

```bash
/opt/kafka_2.13-3.6.1/bin/kafka-storage.sh format -t 你的UUID -c /opt/kafka_2.13-3.6.1/config/kraft/server.properties
```

### 4. 配置 Kafka 系统服务（全节点执行）
创建服务文件：

```bash
vim /usr/lib/systemd/system/kafka.service
```

粘贴内容：

```properties
[Unit]
Description=Apache Kafka Server (Raft mode)
After=network.target

[Service]
Type=forking
User=root
Group=root
ExecStart=/opt/kafka_2.13-3.6.1/bin/kafka-server-start.sh -daemon /opt/kafka_2.13-3.6.1/config/kraft/server.properties
ExecStop=/opt/kafka_2.13-3.6.1/bin/kafka-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

### 5. 启动 Kafka 集群（全节点执行）
```bash
# 重载系统服务
systemctl daemon-reload
# 启动服务
systemctl start kafka
# 设置开机自启
systemctl enable kafka
# 查看运行状态
systemctl status kafka
```

### 6. 创建日志专用 Topic（任意一台节点执行）
```bash
# 创建主题：3分区、3副本，适配集群高可用
/opt/kafka_2.13-3.6.1/bin/kafka-topics.sh --create --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 --replication-factor 3 --partitions 3 --topic nginx-log

# 查看所有主题，验证创建成功
/opt/kafka_2.13-3.6.1/bin/kafka-topics.sh --list --bootstrap-server kafka1:9092
```

---

## 六、node01 专属部署：Nginx(负载均衡) + Filebeat(日志采集)
### 1. 安装并配置 Nginx 负载均衡
#### 1.1 安装 Nginx
```bash
yum install -y nginx
systemctl start nginx
systemctl enable nginx
```

#### 1.2 配置负载均衡 + 日志字段扩展（记录后端节点IP）
编辑主配置文件：

```bash
vim /etc/nginx/nginx.conf
```

替换 `http` 完整区块：

```nginx
http {
    # 日志格式新增 $upstream_addr，记录请求转发的后端节点
    log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for" $upstream_addr';

    access_log  /var/log/nginx/access.log  main;

    sendfile            on;
    tcp_nopush          on;
    tcp_nodelay         on;
    keepalive_timeout   65;
    types_hash_max_size 4096;

    include             /etc/nginx/mime.types;
    default_type        application/octet-stream;
    include /etc/nginx/conf.d/*.conf;

    # 后端服务集群池（node02、node03）
    upstream backend_servers {
        server 172.19.65.56:8080 weight=5;
        server 172.19.65.57:8080 weight=5;
        ip_hash; # 会话保持策略
    }

    server {
        listen       80;
        server_name  _;
        root         /usr/share/nginx/html;

        location / {
            proxy_pass http://backend_servers;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }

        error_page 404 /404.html;
        location = /404.html {}

        error_page 500 502 503 504 /50x.html;
        location = /50x.html {}
    }
}
```

#### 1.3 校验配置并重启 Nginx
```bash
nginx -t
systemctl restart nginx
```

### 2. 部署 Filebeat 采集日志并输出到 Kafka
#### 2.1 配置 Elastic 软件源
```bash
rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch
cat > /etc/yum.repos.d/fb.repo << EOF
[elastic-7.x]
name=Elastic repository for 7.x packages
baseurl=https://artifacts.elastic.co/packages/7.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
EOF
```

#### 2.2 安装 Filebeat
```bash
yum install -y filebeat
```

#### 2.3 修改 Filebeat 主配置
```bash
vim /etc/filebeat/filebeat.yml
```

保留核心配置，清空默认内容后写入：

```yaml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/nginx/access.log
    - /var/log/nginx/error.log

# 输出日志到Kafka集群
output.kafka:
  hosts: ["kafka1:9092","kafka2:9092","kafka3:9092"]
  topic: "nginx-log"
```

#### 2.4 启动 Filebeat
```bash
systemctl start filebeat
systemctl enable filebeat
# 查看状态
systemctl status filebeat
```

#### 2.5 解决 Filebeat 不读取历史日志（常用排错）
Filebeat 默认记录读取位置，清空注册表从头采集：

```bash
systemctl stop filebeat
rm -rf /var/lib/filebeat/registry
systemctl start filebeat
```

---

## 七、node02 专属部署：Logstash + Flask + Redis + Celery 告警
### 1. 部署 Logstash（消费Kafka日志，写入ES）
#### 1.1 安装 Logstash 7.17.21
```bash
cd /opt
wget https://artifacts.elastic.co/downloads/logstash/logstash-7.17.21-x86_64.rpm
rpm -ivh logstash-7.17.21-x86_64.rpm
```

#### 1.2 优化 JVM 内存（适配低配服务器，防止OOM）
```bash
cd /etc/logstash/jvm.options.d/
vim custom-heap.options
```

写入：

```plain
-Xms256m
-Xmx256m
```

#### 1.3 创建日志解析管道配置
```bash
vim /etc/logstash/conf.d/nginx_log_pipeline.conf
```

```nginx
input {
  kafka {
    bootstrap_servers => "kafka1:9092,kafka2:9092,kafka3:9092"
    topics => ["nginx-log"]
    group_id => "logstash-nginx-group"
    codec => json { charset => "UTF-8" }
  }
}

filter {
  grok {
    match => { "message" => "%{IPORHOST:client_ip} - - \[%{HTTPDATE:timestamp}\] \"%{WORD:method} %{URIPATH:path} HTTP/%{NUMBER:http_version}\" %{NUMBER:status:int} %{NUMBER:body_bytes_sent:int} \"%{DATA:referrer}\" \"%{DATA:user_agent}\" %{DATA:backend_node}" }
  }
  date {
    match => [ "timestamp", "dd/MMM/yyyy:HH:mm:ss Z" ]
    target => "@timestamp"
  }
}

output {
  elasticsearch {
    hosts => ["http://kafka1:9201"]
    index => "nginx-logs-%{+YYYY.MM.dd}"
  }
  stdout { codec => rubydebug }
}
```

#### 1.4 配置 Logstash 系统服务
```bash
vim /etc/systemd/system/logstash.service
```

```properties
[Unit]
Description=Logstash
Documentation=https://www.elastic.co/guide/en/logstash/current/index.html
After=network.target

[Service]
User=logstash
Group=logstash
Environment=LS_HOME=/usr/share/logstash
Environment=LS_SETTINGS_DIR=/etc/logstash
Environment=LS_CONF_DIR=/etc/logstash/conf.d
Environment=LS_LOG_DIR=/var/log/logstash
Environment=LS_DATA_DIR=/var/lib/logstash
Environment=PIDFILE=/var/run/logstash/logstash.pid
ExecStart=/usr/share/logstash/bin/logstash --path.settings /etc/logstash
Restart=always
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

#### 1.5 启动 Logstash
```bash
systemctl daemon-reload
# 校验配置
/usr/share/logstash/bin/logstash -t -f /etc/logstash/conf.d/nginx_log_pipeline.conf
# 启动服务
systemctl start logstash
systemctl enable logstash
```

### 2. 部署后端 Web 服务（8080端口）
```bash
yum install -y nginx
sed -i 's/listen       80;/listen       8080;/' /etc/nginx/nginx.conf
echo "I am node02 backend server" > /usr/share/nginx/html/index.html
systemctl start nginx
systemctl enable nginx
```

### 3. 部署 Python + Flask + Redis + Celery 邮件告警
#### 3.1 安装 Python 与依赖
```bash
yum install -y python3 python3-pip
pip3 install flask celery redis kafka-python
```

#### 3.2 部署 Flask 测试服务
```bash
mkdir -p /opt/monitor
vim /opt/monitor/app.py
```

```python
from flask import Flask
app = Flask(__name__)

@app.route('/')
def index():
    return "this is flask web kafka2"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

后台启动：

```bash
python3 /opt/monitor/app.py &
```

#### 3.3 部署 Redis（Celery 消息中间件）
```bash
yum install -y redis
sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis.conf
systemctl start redis
systemctl enable redis
```

#### 3.4 编写 Celery 定时告警任务（解决循环导入）
##### 3.4.1 `celery_app.py`
```bash
vim /opt/monitor/celery_app.py
```

```python
from celery import Celery
from celery.schedules import crontab

app = Celery('tasks', broker='redis://127.0.0.1:6379/1')

app.conf.beat_schedule = {
    'check-kafka-nginx-logs': {
        'task': 'tasks.monitor_kafka_logs',
        'schedule': crontab(minute='*/5'),
    },
}

import tasks
```

##### 3.4.2 `tasks.py`（Kafka日志消费+邮件告警）
```bash
vim /opt/monitor/tasks.py
```

```python
from celery_app import app
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from kafka import KafkaConsumer
import json

KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9092', 'kafka3:9092']
KAFKA_TOPIC = 'nginx-log'
KAFKA_GROUP_ID = 'log-alert-group'

@app.task
def monitor_kafka_logs():
    error_count = 0
    error_logs = []
    threshold = 10
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )

    for msg in consumer:
        log = msg.value
        log_line = log.get('message', str(log))
        if 'error' in log_line.lower():
            error_count += 1
            error_logs.append(log_line)
            if len(error_logs) > 5:
                error_logs.pop(0)

    if error_count > threshold:
        html = f"""
        <html>
        <body>
            <h2>Nginx日志告警</h2>
            <p>告警时间：{now}</p>
            <p>错误日志数量：{error_count}，阈值：{threshold}</p>
            <p>错误日志内容：<br>{'<br>'.join(error_logs)}</p>
        </body>
        </html>
        """
        msg = MIMEText(html, 'html', 'utf-8')
        msg['Subject'] = "【告警】Nginx日志错误数量超标"
        msg['From'] = "你的QQ邮箱@qq.com"
        msg['To'] = "接收邮箱@qq.com"

        try:
            server = smtplib.SMTP_SSL("smtp.qq.com", 465)
            server.login("你的QQ邮箱@qq.com", "邮箱授权码")
            server.sendmail("你的QQ邮箱@qq.com", ["接收邮箱@qq.com"], msg.as_string())
            server.quit()
            print(f"{now} 告警邮件发送成功")
        except Exception as e:
            print(f"邮件发送失败：{e}")
    else:
        print(f"{now} 错误数{error_count}，无需告警")
```

#### 3.5 启动 Celery 服务
```bash
# 停止旧进程
pkill -9 celery
rm -f /opt/monitor/celerybeat-schedule

# 终端1：启动Worker
cd /opt/monitor
celery -A celery_app worker -l info -c 4 &

# 终端2：启动定时任务Beat
cd /opt/monitor
celery -A celery_app beat -l info &
```

---

## 八、node03 专属部署：后端 Web 服务
仅部署后端服务，Kafka 已在前期集群部署完成，无需额外配置

```bash
yum install -y nginx
sed -i 's/listen       80;/listen       8080;/' /etc/nginx/nginx.conf
echo "I am node03 backend server" > /usr/share/nginx/html/index.html
systemctl start nginx
systemctl enable nginx
```

---

## 九、node01 补充部署：Elasticsearch + Kibana（存储+可视化）
### 1. 部署 Elasticsearch 7.17.21
#### 1.1 安装
```bash
cd /opt
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.21-x86_64.rpm
rpm -ivh elasticsearch-7.17.21-x86_64.rpm
```

#### 1.2 内存优化（防止OOM）
```bash
cd /etc/elasticsearch/jvm.options.d/
vim custom-heap.options
```

```plain
-Xms256m
-Xmx256m
```

#### 1.3 修改 ES 主配置
```bash
vim /etc/elasticsearch/elasticsearch.yml
```

```yaml
cluster.name: nginx-log-cluster
node.name: kafka1-node
path.data: /var/lib/elasticsearch
path.logs: /var/log/elasticsearch
network.host: 0.0.0.0
http.port: 9201
discovery.type: single-node
```

#### 1.4 启动 ES
```bash
systemctl start elasticsearch
systemctl enable elasticsearch
# 验证
curl http://127.0.0.1:9201
```

### 2. 部署 Kibana
#### 2.1 安装
```bash
wget https://artifacts.elastic.co/downloads/kibana/kibana-7.17.21-x86_64.rpm
rpm -ivh kibana-7.17.21-x86_64.rpm
```

#### 2.2 修改配置
```bash
vim /etc/kibana/kibana.yml
```

```yaml
server.port: 5601
server.host: "0.0.0.0"
elasticsearch.hosts: ["http://localhost:9201"]
kibana.index: ".kibana"
```

#### 2.3 启动 Kibana
```bash
systemctl start kibana
systemctl enable kibana
```

访问地址：`http://node01公网IP:5601`

### 3. Kibana 索引模式 & 仪表盘制作
1. **创建索引模式**：Stack Management → Index Patterns → 填写 `nginx-logs-*`，时间字段选择 `@timestamp`
2. **制作可视化图表**
    - 请求数趋势图（折线图）
    - 状态码分布饼图（200/404/500）
    - 错误请求TOP10路径（柱状图）
    - 后端节点请求分布饼图（`backend_node` 字段）
3. **组装仪表盘**：将所有图表加入 Dashboard，设置**自动刷新**（1分钟/5分钟），完善面板标

---

## 十、全流程功能验证
### 1. 生成测试日志（node01 执行）
```bash
# 生成正常访问日志
for ((i=1; i<=30; i++));
do
  echo "192.168.1.100 - - [$(date +'%d/%b/%Y:%H:%M:%S %z')] \"GET /index.html HTTP/1.1\" 200 1234" >> /var/log/nginx/access.log
done

# 生成错误日志（触发告警）
for ((i=1; i<=15; i++));
do
  echo "192.168.1.101 - - [$(date +'%d/%b/%Y:%H:%M:%S %z')] ERROR: server internal error $i" >> /var/log/nginx/error.log
done
```

### 2. 链路验证
1. **Filebeat→Kafka**：消费主题查看日志

```bash
/opt/kafka_2.13-3.6.1/bin/kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic nginx-log --from-beginning
```

2. **Kafka→Logstash→ES**：查看ES索引

```bash
curl http://127.0.0.1:9201/_cat/indices?v
```

3. **ES→Kibana**：访问Kibana，查看日志与仪表盘数据
4. **Celery告警**：等待5分钟定时任务，查看邮箱是否收到告警邮件
5. **负载均衡**：多次访问 `http://node01公网IP`，查看 node02/node03 页面交替切换

---

## 十一、项目总结 
### 一、项目用到的核心技术
本项目采用 **Filebeat+Kafka+ELK+Celery** 技术栈，搭配 Nginx 负载均衡，搭建了一套企业级 Nginx 日志全链路处理平台，用到的核心技术包括：

+ 日志采集：Filebeat
+ 消息队列：Kafka 3节点Raft集群
+ 日志解析：Logstash
+ 数据存储：Elasticsearch
+ 可视化监控：Kibana
+ 定时告警：Celery + Redis
+ 负载均衡：Nginx
+ 系统服务管理：Systemd

---

### 二、项目解决的核心问题
1. **日志数据可靠性问题**：通过 Kafka 3节点集群实现日志多副本存储，规避了单节点故障导致的日志丢失、传输中断风险；
2. **突发流量稳定性问题**：Filebeat 轻量采集 + Kafka 削峰填谷，避免日志流量压垮下游组件，提升平台整体稳定性；
3. **日志排查效率低问题**：ELK 平台将日志集中存储并可视化，通过仪表盘快速查看请求量、状态码分布、错误路径，替代手动翻日志；
4. **问题发现不及时问题**：Celery 定时检测异常日志并发送邮件告警，实现问题早发现、早处理，不用24小时守着服务器；
5. **后端服务负载不均问题**：Nginx 负载均衡实现流量分发，会话保持策略适配有状态业务，让多后端服务均匀分担压力；
6. **低配服务器稳定性问题**：Systemd 服务管理 + JVM 内存优化，解决了服务无法自启、故障后无法恢复，以及组件因内存不足崩溃的问题。

---

### 三、项目中遇到的问题及解决方法
1. **Kafka 单节点部署存在单点故障风险**  
解决：改用3节点Raft模式集群，摒弃Zookeeper，配置多副本存储，规避单点故障，提升日志数据可靠性。
2. **日志突发流量容易压垮 Logstash，导致服务崩溃**  
解决：在 Filebeat 和 Logstash 之间加入 Kafka 消息队列，作为缓冲层削峰填谷，应对突发流量，提升平台稳定性。
3. **日志是纯文本格式，无法直接分析，排查问题效率极低**  
解决：通过 Logstash 的 Grok 过滤器解析非结构化日志，提取客户端IP、请求路径、状态码等关键字段，存入 Elasticsearch；再通过 Kibana 制作可视化仪表盘，直观展示日志数据，大幅提升排查效率。
4. **无法主动发现日志异常，只能等用户反馈问题**  
解决：基于 Celery 开发定时日志检测任务，定时消费 Kafka 中的日志数据，统计错误日志数量；当错误数超过阈值时，自动发送邮件告警，实现问题主动发现。
5. **后端服务压力不均，部分节点负载过高，会话无法保持**  
解决：配置 Nginx 负载均衡，采用 ip_hash 会话保持策略，将用户请求均匀分发到多台后端服务器，同时保证会话一致性，提升业务可用性。
6. **低配服务器上 Elasticsearch、Logstash 频繁因内存不足崩溃（OOM）**  
解决：优化 JVM 内存参数，将堆内存设置为256m，适配低配服务器资源；同时配置 Systemd 服务，实现服务开机自启与故障自动重启，保障服务持续运行。





