#!/usr/bin/env bash
go build  -o raftexample ../server/raftexample.go 
go build ../client/
go build ../applydb/dbapply.go