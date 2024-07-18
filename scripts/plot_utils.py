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

# Sample log: fork.last = 610405, commit_index = 610397, byz_commit_index = 0, pending_acks = 8, num_txs = 662124, fork.last_hash = 8cf02230d5dfe0275b613b8b34493b90e723c80350eb71289e941b226a13a8a8
node_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] fork\.last = ([0-9]+), commit_index = ([0-9]+), byz_commit_index = ([0-9]+), pending_acks = ([0-9]+), num_txs = ([0-9]+), fork.last_hash = (.+)")

# Sample log: Client Id: 48, Msg Id: 27000, Latency: 846 us
client_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Client Id\: ([0-9]+), Msg Id\: ([0-9]+), Latency\: ([.0-9]+) us")


def process_tput(points, ramp_up, ramp_down, tputs, tputs_unbatched):
    points = [
        (
            isoparse(a[0]),    # ISO format is used in run_remote
            int(a[1]),         # fork.last
            int(a[2]),         # commit_index
            int(a[3]),         # byz_commit_index
            int(a[4]),         # pending_acks
            int(a[5]),         # num_txs,
            a[6],              # fork.last_hash
        )
        for a in points
    ]

    # Filter points, only keep if after ramp_up time and before ramp_down time

    start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
    end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

    points = [p for p in points if p[0] >= start_time and p[0] <= end_time]

    total_runtime = (points[-1][0] - points[0][0]).total_seconds()
    total_commit = points[-1][1] - points[0][1]
    total_tx = points[-1][5] - points[0][5]

    tputs.append(total_tx / total_runtime)
    tputs_unbatched.append(total_commit / total_runtime)


def process_latencies(points, ramp_up, ramp_down, latencies):
    points = [
        (
            isoparse(a[0]),      # ISO format is used in run_remote
            int(a[1]),           # Client Id
            int(a[2]),           # Msg Id
            float(a[3]),         # Latency us
        )
        for a in points
    ]

    # Filter points, only keep if after ramp_up time and before ramp_down time

    start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
    end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

    points = [p for p in points if p[0] >= start_time and p[0] <= end_time]

    latencies.extend([p[3] for p in points])



Stats = namedtuple("Stats", [
    "mean_tput", "stdev_tput",
    "mean_tput_unbatched", "stdev_tput_unbatched",
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
                if len(captures) == 1:
                    points.append(captures[0])
        process_tput(points, ramp_up, ramp_down, tputs, tputs_unbatched)

    for i in range(repeats):
        points = []
        for c in range(1, num_clients + 1):
            try:
                with open(f"{dir}/{i}/client{c}.log", "r") as f:
                    for line in f.readlines():
                        captures = client_rgx.findall(line)
                        if len(captures) == 1:
                            points.append(captures[0])
            except:
                pass
        process_latencies(points, ramp_up, ramp_down, latencies)

    
    return Stats(
        mean_tput=mean(tputs),
        stdev_tput=stdev(tputs),
        mean_tput_unbatched=mean(tputs_unbatched),
        stdev_tput_unbatched=stdev(tputs_unbatched),
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
    res = parse_log_dir(dir, repeats, num_clients, leader, ramp_up, ramp_down)

    return {sig_delay: res}


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


def plot_tput_bar_graph(stat_list: Dict[str, Stats], xlabel, name):
    plt.bar(
        list(stat_list.keys()),
        [p.mean_tput for p in stat_list.values()]
    )
    plt.xlabel(xlabel)
    plt.ylabel("Throughput (tx/s)")

    plt.grid()
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
    
