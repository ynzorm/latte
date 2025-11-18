docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

docker run --name scylla-node1 -d scylladb/scylla --alternator-write-isolation always --alternator-port 8000
docker run --name scylla-node2 -d scylladb/scylla --alternator-write-isolation always --alternator-port 8000 --seeds="$(docker inspect --format='{{index .NetworkSettings.Networks.bridge.IPAddress }}' scylla-node1)"
docker run --name scylla-node3 -d scylladb/scylla --alternator-write-isolation always --alternator-port 8000 --seeds="$(docker inspect --format='{{index .NetworkSettings.Networks.bridge.IPAddress }}' scylla-node1)"

timeout=300
until docker exec scylla-node1 nodetool status >/dev/null 2>&1 || [ $timeout -le 0 ]; do
echo "Waiting for Scylla cluster to become ready..."
sleep 15
timeout=$((timeout-15))
done

if [ $timeout -le 0 ]; then
echo "Scylla cluster never became ready"
exit 1
fi
echo "Scylla cluster is ready!"