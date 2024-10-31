# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.


.PHONY: pirateship_logger
pirateship_logger:
	CC=clang CXX=clang++ cargo build --release


.PHONY: pirateship_logger_nofast
pirateship_logger_nofast:
	CC=clang CXX=clang++ cargo build --release --features pirateship,app_logger,storage,platforms --no-default-features


.PHONY: lucky_raft_logger
lucky_raft_logger:
	CC=clang CXX=clang++ cargo build --release --features lucky_raft,app_logger,storage --no-default-features


.PHONY: signed_raft_logger
signed_raft_logger:
	CC=clang CXX=clang++ cargo build --release --features signed_raft,app_logger,storage --no-default-features


.PHONY: diverse_raft_logger
diverse_raft_logger:
	CC=clang CXX=clang++ cargo build --release --features diverse_raft,app_logger,storage --no-default-features


.PHONY: jolteon_logger
jolteon_logger:
	CC=clang CXX=clang++ cargo build --release --features jolteon,app_logger,storage,fast_path --no-default-features


.PHONY: chained_pbft_logger
chained_pbft_logger:
	CC=clang CXX=clang++ cargo build --release --features chained_pbft,app_logger,storage,fast_path --no-default-features

.PHONY: bench
bench:
	CC=clang CXX=clang++ cargo bench

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

