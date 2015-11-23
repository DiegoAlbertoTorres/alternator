#!/bin/bash

# Terminal to be used. Preferably one that supports TRUECOLOR.
export ALTERM=konsole

# Create a ring with a good distribution
go install git/alternator
i3-msg workspace next
$ALTERM -e alternator --port=38650 #00d9b71df2fa508d8e25689b05b3eb5795c0c907
sleep 2
$ALTERM -e alternator --join=38650 --port=34001 & #28790d504e6a2dd2061d0db6336421453048354c
sleep 0.2
$ALTERM -e alternator --join=38650 --port=50392 & #4b911cfc78ff5b93164f39981db0efb279ab5db2
sleep 0.2
$ALTERM -e alternator --join=38650 --port=43960 & #771d26df1732ac17709ed5cc6913b426423cda5a
sleep 0.2
$ALTERM -e alternator --join=38650 --port=56083 & #9fb6be4d3f4b7b9411abe7b5c4adbbe057119e5f
sleep 0.2
$ALTERM -e alternator --join=38650 --port=54487 & #b2f4defc5f806d98141a823d9c37223c90ab3df5
sleep 0.2
$ALTERM -e alternator --join=38650 --port=56043 & #d0522c345c2a56d723554dab53230f04b983af9d
sleep 0.2
$ALTERM -e alternator --join=38650 --port=33846 & #fe9ef6131b9a934a55560a8054dd0c8adb05cba4
sleep 8
i3-msg workspace prev

# Put some keys
alternator --port=38650 --command=Put a 1 fe9ef6131b9a934a55560a8054dd0c8adb05cba4 4b911cfc78ff5b93164f39981db0efb279ab5db2
alternator --port=38650 --command=Put b 2 4b911cfc78ff5b93164f39981db0efb279ab5db2 771d26df1732ac17709ed5cc6913b426423cda5a
alternator --port=38650 --command=Put c 3 771d26df1732ac17709ed5cc6913b426423cda5a 9fb6be4d3f4b7b9411abe7b5c4adbbe057119e5f
alternator --port=38650 --command=Put d 4 9fb6be4d3f4b7b9411abe7b5c4adbbe057119e5f b2f4defc5f806d98141a823d9c37223c90ab3df5
alternator --port=38650 --command=Put e 5 b2f4defc5f806d98141a823d9c37223c90ab3df5 d0522c345c2a56d723554dab53230f04b983af9d
alternator --port=38650 --command=Put f 6 d0522c345c2a56d723554dab53230f04b983af9d fe9ef6131b9a934a55560a8054dd0c8adb05cba4

# Get keys
