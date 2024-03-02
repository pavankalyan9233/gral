#!/usr/bin/fish
target/debug/gral &
jobs
sleep 20
kill -INT %1
