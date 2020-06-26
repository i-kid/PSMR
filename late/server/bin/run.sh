sudo rm -rf raftexample*
sudo rm -rf *.db
echo "all you want is deleted, please wait...."
bash ./build.sh
echo "no matter you change source code or not, the app has been rebuild"
#redis-benchmark -n 10000  -q -t SET,GET -c 10
#go tool pprof --pdf ./raftexample /tmp/profile949669893/cpu.pprof > cpu.pdf
goreman start