#!/usr/bin/env bash
RUN_NAME="tool"
RUN_NAME_LINUX="tool_linux"

mkdir -p output

export GO111MODULE="on"
export GOPROXY="https://goproxy.io,direct"
export GOSUMDB="sum.golang.google.cn"

go build -o output/${RUN_NAME}
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o output/${RUN_NAME_LINUX}
