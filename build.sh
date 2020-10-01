CGO_ENABLED=0 go build -mod=vendor
docker build -t registry_relay -f Dockerfile_relay .
