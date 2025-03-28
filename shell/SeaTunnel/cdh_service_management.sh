#!/bin/bash

# 定义操作类型，start 或 stop
ACTION=$1

# 定义 CDH 服务列表，根据实际情况调整
SERVICES=(
    "cloudera-scm-agent"
)

# 函数：启动服务
start_services() {
    for service in "${SERVICES[@]}"; do
        echo "Starting $service..."
        systemctl start $service
        if [ $? -eq 0 ]; then
            echo "$service started successfully."
        else
            echo "Failed to start $service."
        fi
    done
}

# 函数：停止服务
stop_services() {
    for service in "${SERVICES[@]}"; do
        echo "Stopping $service..."
        systemctl stop $service
        if [ $? -eq 0 ]; then
            echo "$service stopped successfully."
        else
            echo "Failed to stop $service."
        fi
    done
}

# 根据操作类型执行相应操作
case $ACTION in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    *)
        echo "Usage: $0 [start|stop]"
        exit 1
        ;;
esac
