#!/bin/bash
export PATH="$HOME/go/bin:$PATH"
protoc --go_out=. --go_opt=paths=source_relative ./lib/yfs.proto