from typing import Dict, List
import click

from collections import namedtuple
import re
import datetime
from dateutil.parser import isoparse
from statistics import mean, median, stdev, quantiles

import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint

# Log format follows the log4rs config.
# Capture the time from the 3rd []

# Sample log: [INFO][pft::execution::engines::logger][2024-08-06T10:28:13.926997933+00:00] fork.last = 2172, fork.last_qc = 2169, commit_index = 2171, byz_commit_index = 2166, pending_acks = 200, pending_qcs = 1 num_txs = 388305, fork.last_hash = b7da989badce213929ab457e5301b587593e0781e081ba7261d57cd7778e1b7b, total_client_request = 388706, view = 1, view_is_stable = true, i_am_leader: true
node_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] fork\.last = ([0-9]+), fork\.last_qc = ([0-9]+), commit_index = ([0-9]+), byz_commit_index = ([0-9]+), pending_acks = ([0-9]+), pending_qcs = ([0-9]+) num_txs = ([0-9]+), fork\.last_hash = (.+), total_client_request = ([0-9]+), view = ([0-9]+), view_is_stable = (.+), i_am_leader\: (.+)")

# Sample log: [INFO][client][2024-08-06T10:28:12.352816849+00:00] Client Id: 264, Msg Id: 224, Block num: 1000, Tx num: 145, Latency: 4073 us, Current Leader: node1
client_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Client Id\: ([0-9]+), Msg Id\: ([0-9]+), Block num\: ([0-9]+), Tx num\: ([0-9]+), Latency\: ([.0-9]+) us, Current Leader\: (.+)")


def process_tput(points, ramp_up, ramp_down, tputs, tputs_unbatched):
    points = [
        (
            isoparse(a[0]),    # ISO format is used in run_remote
            int(a[1]),         # fork.last
            int(a[2]),         # fork.last_qc
            int(a[3]),         # commit_index
            int(a[4]),         # byz_commit_index
            int(a[5]),         # pending_acks
            int(a[6]),         # pending_qcs
            int(a[7]),         # num_txs,
            a[8],              # fork.last_hash,
            int(a[9]),         # total_client_request
            int(a[10]),        # view
            a[11] == "true",   # view_is_stable
            a[12] == "true"    # i_am_leader
        )
        for a in points
    ]

    # Filter points, only keep if after ramp_up time and before ramp_down time

    start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
    end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

    points = [p for p in points if p[0] >= start_time and p[0] <= end_time]

    total_runtime = (points[-1][0] - points[0][0]).total_seconds()
    total_commit = points[-1][3] - points[0][3]
    total_tx = points[-1][7] - points[0][7]

    tputs.append(total_tx / total_runtime)
    tputs_unbatched.append(total_commit / total_runtime)


def process_latencies(points, ramp_up, ramp_down, latencies):
    points = [
        (
            isoparse(a[0]),      # ISO format is used in run_remote
            int(a[1]),           # Client Id
            int(a[2]),           # Msg Id
            int(a[3]),           # Block num
            int(a[4]),           # Tx num
            float(a[5]),         # Latency us
            a[6]                 # Current Leader
        )
        for a in points
    ]
    total_n = len(points)

    # Filter points, only keep if after ramp_up time and before ramp_down time

    start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
    end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

    points = [p for p in points if p[0] >= start_time and p[0] <= end_time]

    latencies.extend([p[3] for p in points])



Stats = namedtuple("Stats", [
    "mean_tput", "stdev_tput",
    "mean_tput_unbatched", "stdev_tput_unbatched",
    "latency_prob_dist",
    "mean_latency", "median_latency", "p25_latency", "p75_latency", "p99_latency", "max_latency", "min_latency", 
    "stdev_latency"])

def parse_log_dir(dir, repeats, num_clients, leader, ramp_up, ramp_down) -> Stats:
    
    tputs = []
    tputs_unbatched = []
    latencies = []
    
    for i in range(repeats):
        points = []
        with open(f"{dir}/{i}/{leader}.log", "r") as f:
            for line in f.readlines():
                captures = node_rgx.findall(line)
                # print(captures)
                if len(captures) == 1:
                    points.append(captures[0])
        try:
            process_tput(points, ramp_up, ramp_down, tputs, tputs_unbatched)
        except:
            continue

    for i in range(repeats):
        points = []
        for c in range(1, num_clients + 1):
            try:
                with open(f"{dir}/{i}/client{c}.log", "r") as f:
                    for line in f.readlines():
                        captures = client_rgx.findall(line)
                        # print(captures)
                        if len(captures) == 1:
                            points.append(captures[0])
            except:
                pass
        try:
            process_latencies(points, ramp_up, ramp_down, latencies)
        except:
            continue

    latency_prob_dist = np.array(latencies)
    latency_prob_dist.sort()
    # latency_prob_dist = latency_prob_dist.clip(latency_prob_dist[0], quantiles(latencies, n=100)[98])
    p = 1. * np.arange(len(latency_prob_dist)) / (len(latency_prob_dist) - 1)

    return Stats(
        mean_tput=mean(tputs),
        stdev_tput=stdev(tputs),
        mean_tput_unbatched=mean(tputs_unbatched),
        stdev_tput_unbatched=stdev(tputs_unbatched),
        latency_prob_dist=(latency_prob_dist, p),
        mean_latency=mean(latencies),
        median_latency=median(latencies),
        p25_latency=quantiles(latencies, n=100)[24],
        p75_latency=quantiles(latencies, n=100)[74],
        p99_latency=quantiles(latencies, n=100)[98],
        max_latency=max(latencies),
        min_latency=min(latencies),
        stdev_latency=stdev(latencies)
    )


def parse_log_dir_with_total_clients(dir, repeats, num_clients, leader, ramp_up, ramp_down) -> Dict[int, Stats]:
    with open(f"{dir}/num_clients.txt") as f:
        total_clients = int(f.read().strip())
    res = parse_log_dir(dir, repeats, num_clients, leader, ramp_up, ramp_down)

    return {total_clients: res}

def parse_log_dir_with_sig_delay(dir, repeats, num_clients, leader, ramp_up, ramp_down) -> Dict[str, Stats]:
    with open(f"{dir}/sig_sweep.txt") as f:
        sig_delay = str(f.read().strip())

    try:
        res = parse_log_dir(dir, repeats, num_clients, leader, ramp_up, ramp_down)

        return {sig_delay: res}
    except:
        # Failed run
        return {}


def plot_tput_vs_latency(stats: Dict[int, Stats], name: str):
    points = list(sorted(stats.items()))

    mean_tputs = [p[1].mean_tput for p in points]
    stdev_tputs = [p[1].stdev_tput for p in points]
    median_latencies = [p[1].median_latency for p in points]
    yerr_max = [p[1].p75_latency - p[1].median_latency for p in points]
    yerr_min = [p[1].median_latency - p[1].p25_latency for p in points]

    plt.errorbar(
        np.array(mean_tputs),
        np.array(median_latencies),
        yerr=[yerr_min, yerr_max],
        xerr=np.array(stdev_tputs),
    )
    plt.xlabel("Throughput (req/s)")
    plt.ylabel("Latency (us)")
    plt.grid()
    
    plt.savefig(name)

def plot_tput_vs_latency_multi(stat_list: List[Dict[int, Stats]], legends: List[str], name: str):
    assert len(stat_list) == len(legends)
    
    for i, stats in enumerate(stat_list): 
        points = list(sorted(stats.items()))

        mean_tputs = [p[1].mean_tput for p in points]
        stdev_tputs = [p[1].stdev_tput for p in points]
        mean_latencies = [p[1].mean_latency for p in points]
        stdev_latencies = [p[1].stdev_latency for p in points]
        yerr_max = [p[1].p75_latency - p[1].median_latency for p in points]
        yerr_min = [p[1].median_latency - p[1].p25_latency for p in points]

        plt.errorbar(
            x=np.array(mean_tputs),
            y=np.array(mean_latencies),
            yerr=[yerr_min, yerr_max],
            xerr=np.array(stdev_tputs),
            label=legends[i],
            marker='o',
            ecolor='r',
            capsize=5
        )
    
    
    plt.xlabel("Throughput (req/s)")
    plt.ylabel("Latency (us)")
    plt.grid()
    plt.legend()
    plt.savefig(name)


def plot_tput_bar_graph(stat_list: Dict[str, Stats], name):
    plt.bar(
        list(stat_list.keys()),
        [p.mean_tput for p in stat_list.values()]
    )
    plt.ylabel("Throughput (tx/s)")
    plt.xticks(rotation=45)

    plt.grid()
    plt.savefig(name, bbox_inches='tight')

def plot_latency_cdf(stat_list: Dict[str, Stats], name):
    max_p99 = max([v.p99_latency for v in stat_list.values()])
    for k, v in stat_list.items():
        plt.plot(v.latency_prob_dist[0], v.latency_prob_dist[1], label=k)
    
    plt.xlim(0, max_p99)
    plt.grid()
    plt.legend()

    plt.savefig(name)


@click.command()
@click.option(
    "-d", "--dir", required=True, multiple=True,
    type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
@click.option(
    "-r", "--repeats", required=True,
    type=click.INT
)
@click.option(
    "-c", "--num_clients", required=True,
    type=click.INT
)
@click.option(
    "-l", "--leader", required=True,
    type=click.STRING
)
@click.option(
    "-up", "--ramp_up", required=True,
    type=click.INT
)
@click.option(
    "-down", "--ramp_down", required=True,
    type=click.INT
)
@click.option(
    "-o", "--out", required=True,
    type=click.STRING
)
@click.option(
    "--legend", multiple=True,
    default=[],
    type=click.STRING
)
def main(dir, repeats, num_clients, leader, ramp_up, ramp_down, out, legend):
    if len(legend) == 0:
        stats = {}
        for d in dir:
            res = parse_log_dir_with_total_clients(d, repeats, num_clients, leader, ramp_up, ramp_down)
            stats.update(res)

        pprint(stats)
        plot_tput_vs_latency(stats, out)
    else:
        assert len(dir) % len(legend) == 0
        per_legend = len(dir) // len(legend)
        stats = [{} for _ in range(len(legend))]
        for i, d in enumerate(dir):
            res = parse_log_dir_with_total_clients(d, repeats, num_clients, leader, ramp_up, ramp_down)
            stats[i // per_legend].update(res)

        pprint(stats)
        plot_tput_vs_latency_multi(stats, legend, out)

        



if __name__ == "__main__":
    main()
    
