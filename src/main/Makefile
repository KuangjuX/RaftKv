UserProg = crash.go
ExecFile = crash.so

.PHONY: build master worker clean

build:
	@go build -race -buildmode=plugin ../mrapps/$(UserProg)

master:
	@go run -race mrcoordinator.go pg-*.txt

worker: build
	@go run -race mrworker.go $(ExecFile) & \
	go run -race mrworker.go $(ExecFile) & \
	go run -race mrworker.go $(ExecFile)

clean:
	@rm $(ExecFile)
	@rm map-*
	@rm -rf mr-*