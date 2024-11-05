export GLOG_logtostderr=1

killall coordinator
killall tsd
killall tsc

make

# start the coordinator
./coordinator &
# wait for the coordinator to start
sleep 2
# start cluster 1
./tsd &
# start cluster 2 on port 3011
./tsd -c 2 -p 3011 &
