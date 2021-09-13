UserProg = wc.go
ExecFile = wc.so

.PHONY: build master worker clean

build:
	@go build -race -buildmode=plugin ../mrapps/$(UserProg)

master:
	@go run -race mrcoordinator.go pg-*.txt

worker: build
	@go run -race mrworker.go $(ExecFile)

clean:
	@rm $(ExecFile)
	@rm map-*
	@rm mr-out*