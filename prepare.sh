#!/bin/bash

# Install Go dependencies for the project
go mod tidy

# Install necessary tools
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
