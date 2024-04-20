main:
	go build worker.go database.go -o mapreduce


run:
	./mapreduce