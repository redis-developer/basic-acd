if [ ! -f $PWD/haproxy/server.pem ]
then
    echo "*** Generate Self-signed Cert ***"
    openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -sha256 -days 3650 -nodes -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=CommonNameOrHostname"
    bash -c 'cat server.key server.crt >> ./haproxy/server.pem'
    rm server.key server.crt
fi

docker compose --profile redis up -d

echo "*** Wait for RE to come up ***"
curl -s -o /dev/null --retry 5 --retry-all-errors --retry-delay 3 -f -k -u "redis@redis.com:redis" https://192.168.20.2:9443/v1/bootstrap

echo "*** Build Cluster ***"
docker exec -it re1 /opt/redislabs/bin/rladmin cluster create name cluster.local username redis@redis.com password redis
docker exec -it re2 /opt/redislabs/bin/rladmin cluster join nodes 192.168.20.2 username redis@redis.com password redis
docker exec -it re3 /opt/redislabs/bin/rladmin cluster join nodes 192.168.20.2 username redis@redis.com password redis
docker exec -it re1 /opt/redislabs/bin/rladmin cluster config handle_redirects enabled

echo "*** Start Load Balancers ***"
docker compose --profile loadbalancer up -d

echo "*** Wait for VIP to come up ***"
while ! ping -c 1 192.168.20.100 > /dev/null
do
    sleep 1
done

echo "*** Build RE DB ***"
curl -s -o /dev/null -k -u "redis@redis.com:redis" https://192.168.20.100:9443/v1/bdbs -H "Content-Type:application/json" -d @$PWD/redis/redb.json

echo "*** Start Rest API Servers  ***"
docker compose --profile rest up -d

echo "*** Start Dispatchers ***"
docker compose --profile dispatcher up -d