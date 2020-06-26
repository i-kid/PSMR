#!/bin/bash

# sum=6379
# for ((i=1; i<=100; i++))
# do
#     ((sum += i))
# done
# echo "The sum is: $sum"
sum=6379
temp=0
for ((i=1;i<=7;i++))
do
   ((temp = sum+i))
   echo "$temp"
   redis-server /etc/redis/redis$temp.conf
done