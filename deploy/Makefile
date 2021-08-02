ifndef version
	version=latest
endif

default:
	docker build -t bifrost-nginx:$(version) -f ./Dockerfile .
	docker tag bifrost-nginx:$(version) hongkangzy/bifrost-nginx:$(version)
	docker push hongkangzy/bifrost-nginx:$(version)
