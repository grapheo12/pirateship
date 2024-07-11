.PHONY: all
all:
	CC=clang CXX=clang++ cargo build --release

.PHONY: clean
clean:
	rm -f perf.data perf.data.old
	rm -rf *.log logs/

.PHONY: install-deps-ubuntu
install-deps-ubuntu:
	apt update
	apt install -y build-essential cmake clang llvm pkg-config
	apt install -y jq
	apt install -y protobuf-compiler
	apt install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`
	apt install -y net-tools
	apt install -y ca-certificates curl libssl-dev

