rm -rf serproxy*

sleep 3

./build.sh

curl -L http://127.0.0.1:12380/2 -XPOST -d http://127.0.0.1:12377
curl -L http://127.0.0.1:22380/2 -XPOST -d http://127.0.0.1:22377
curl -L http://127.0.0.1:32380/2 -XPOST -d http://127.0.0.1:32377

sleep 3
goreman start
sleep 3
goreman start
