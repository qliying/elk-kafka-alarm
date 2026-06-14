#!/bin/bash

# 一键测试脚本：同时制造网卡流量 + Nginx日志（正常+错误）

echo "===== 启动持续访问，制造网卡流量和200日志 ====="
# 后台持续访问本地Nginx，制造流量和正常请求
while true; do curl -s http://127.0.0.1 > /dev/null; sleep 0.1; done &
LOOP_PID=$!
echo "后台循环访问已启动，PID: $LOOP_PID"

echo "===== 制造Nginx错误请求（404） ====="
# 批量访问不存在路径，生成404日志
for i in {1..15};do
    curl -s http://127.0.0.1/test_err_$RANDOM > /dev/null
    sleep 0.2
done

echo "===== 等待10秒，让网卡流量被监控脚本采样 ====="
sleep 10

echo "===== 执行监控任务，发送测试邮件 ====="
# 执行你的监控脚本任务（路径按你的项目目录调整）
python3 -c "from tasks import monitor_nginx_traffic; monitor_nginx_traffic.delay()"

echo "===== 测试完成，停止后台循环访问 ====="
kill $LOOP_PID
echo "已停止后台访问进程，测试结束"
