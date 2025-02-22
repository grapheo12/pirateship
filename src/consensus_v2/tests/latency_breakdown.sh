#!/bin/bash

set -e
# set -o xtrace

echo "Running test"
cargo test --lib consensus_v2 --release\
    -- consensus_v2::tests::integration_tests::test_client_reply --exact --nocapture\
    > /tmp/latency_breakdown.log 2>&1

echo "Test completed"

TOTAL_TIME=0

GET_AVG_TIME() {
    val=$(grep "$1" /tmp/latency_breakdown.log | tail -n 1 | cut -d',' -f 2 | cut -d' ' -f 4)
    TOTAL_TIME=$((TOTAL_TIME + val))
    echo "$2: $val us"
}

GET_AVG_TIME_DBG() {
    grep "$1" /tmp/latency_breakdown.log
}

# Signed block critical path
echo "Breakdown for critical path of signed block"
GET_AVG_TIME "\[Perf:BatchProposer\] Event: Add request to batch" "Add request to batch"
GET_AVG_TIME "\[Perf:BatchProposer\] Event: Propose batch" "Propose batch"
GET_AVG_TIME "\[Perf:BlockSequencerSigned\] Event: Add QCs" "Add QCs"
GET_AVG_TIME "\[Perf:BlockSequencerSigned\] Event: Create Block" "Create Block"
GET_AVG_TIME "\[Perf:BlockSequencerSigned\] Event: Send to Client Reply" "Sequencer sends to Client Reply"
GET_AVG_TIME "\[Perf:BlockSequencerSigned\] Event: Send to Block Broadcaster" "Sequencer sends to reference Block Broadcaster"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockSigned\] Event: Serialize without parent hash" "Serialize partial"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockSigned\] Event: Hash Partial" "Hash partial"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockSigned\] Event: Add parent hash" "Add parent hash"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockSigned\] Event: Sign" "Sign"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockSigned\] Event: Send" "Send to Block Broadcaster"
GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Store block" "Forward to Storage"
GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Forward block to logserver" "Forward block to logserver"
GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Forward block to staging" "Forward block to staging"
GET_AVG_TIME "\[Perf:LeaderStagingSigned\] Event: Vote to Self" "Vote to Self"
GET_AVG_TIME "\[Perf:LeaderStagingSigned\] Event: Crash Commit" "Crash Commit"
GET_AVG_TIME "\[Perf:LeaderStagingSigned\] Event: Send Crash Commit to App" "Send Crash Commit to App"
GET_AVG_TIME "\[Perf:Application\] Event: Process Crash Committed Block" "Process Crash Committed Block"
GET_AVG_TIME "\[Perf:Application\] Event: Send Reply" "Send to Reply Handler"


# GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Serialize" "Serialize for network"
# GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Forward block to other nodes" "Network Broadcast"

echo "Total time: $TOTAL_TIME us"
grep "latency" /tmp/latency_breakdown.log

echo "=========================================="
TOTAL_TIME=0
# Unsigned block critical path
echo "Breakdown for critical path of unsigned block"
GET_AVG_TIME "\[Perf:BatchProposer\] Event: Add request to batch" "Add request to batch"
GET_AVG_TIME "\[Perf:BatchProposer\] Event: Propose batch" "Propose batch"
GET_AVG_TIME "\[Perf:BlockSequencerUnsigned\] Event: Add QCs" "Add QCs"
GET_AVG_TIME "\[Perf:BlockSequencerUnsigned\] Event: Create Block" "Create Block"
GET_AVG_TIME "\[Perf:BlockSequencerUnsigned\] Event: Send to Client Reply" "Sequencer sends to Client Reply"
GET_AVG_TIME "\[Perf:BlockSequencerUnsigned\] Event: Send to Block Broadcaster" "Sequencer sends to reference Block Broadcaster"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockUnsigned\] Event: Serialize without parent hash" "Serialize partial"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockUnsigned\] Event: Hash Partial" "Hash partial"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockUnsigned\] Event: Add parent hash" "Add parent hash"
# GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockUnsigned\] Event: Sign" "Sign"
GET_AVG_TIME "\[Perf:CryptoWorker.*:PrepareBlockUnsigned\] Event: Send" "Send to Block Broadcaster"
GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Store block" "Forward to Storage"
GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Forward block to logserver" "Forward block to logserver"
GET_AVG_TIME "\[Perf:BlockBroadcasterMyBlock\] Event: Forward block to staging" "Forward block to staging"
GET_AVG_TIME "\[Perf:LeaderStagingUnsigned\] Event: Vote to Self" "Vote to Self"
GET_AVG_TIME "\[Perf:LeaderStagingUnsigned\] Event: Crash Commit" "Crash Commit"
GET_AVG_TIME "\[Perf:LeaderStagingUnsigned\] Event: Send Crash Commit to App" "Send Crash Commit to App"
GET_AVG_TIME "\[Perf:Application\] Event: Process Crash Committed Block" "Process Crash Committed Block"
GET_AVG_TIME "\[Perf:Application\] Event: Send Reply" "Send to Reply Handler"

echo "Total time: $TOTAL_TIME us"
grep "latency" /tmp/latency_breakdown.log

echo "=========================================="

grep "hroughput" /tmp/latency_breakdown.log

