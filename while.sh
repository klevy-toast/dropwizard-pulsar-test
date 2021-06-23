#!/bin/bash
while :
do
  echo "curl"
  curl http://localhost:18080/test?num=10000
  sleep 1
done