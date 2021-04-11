build:
	cd microservice1 && GOOS=linux GOARCH=amd64 go build -o ../bin/microservice1
	cd microservice2 && GOOS=linux GOARCH=amd64 go build -o ../bin/microservice2

clean:
	rm -rf bin/*

run1:
	go run microservice1/producer.go

run2:
	go run microservice2/consumer.go