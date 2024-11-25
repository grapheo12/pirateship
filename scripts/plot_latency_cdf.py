# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

import datetime
from plot_utils import node_rgx, isoparse, parse_log_dir_with_total_clients, process_latencies, client_rgx
import matplotlib.pyplot as plt
import numpy as np
import re
import click
from pprint import pprint
import os
from statistics import quantiles
import matplotlib


def parse_only_client_latencies(dir, num_clients, ramp_up, ramp_down, ):
    points = []
    latencies = []
    for c in range(1, num_clients + 1):
        try:
            with open(f"{dir}/0/client{c}.log", "r") as f:
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
        pass

    latency_prob_dist = np.array(latencies)
    latency_prob_dist.sort()
    # latency_prob_dist = latency_prob_dist.clip(latency_prob_dist[0], quantiles(latencies, n=100)[98])
    p = 1. * np.arange(len(latency_prob_dist)) / (len(latency_prob_dist) - 1)

    return (latencies, (latency_prob_dist, p))


def plot_all_latency_cdfs(path, start, end, num_clients, ramp_up, ramp_down, out, legend, skip):
    dir = list(os.scandir(path))
    dir = [str(p.path) for p in dir if p.is_dir()]
    dir = dir[:len(legend)]

    start = isoparse(start)
    end = isoparse(end)
    dir = [p for p in dir if start <= isoparse(os.path.basename(p)) <= end]
    dir.sort(key=lambda x: isoparse(os.path.basename(x)))
    pprint(dir)

    print(len(dir), start, end)

    max_p99 = 0
    min_min = float('inf')
    font = {
        'size'   : 20}
    matplotlib.rc('font', **font)
    for i, d in enumerate(dir):
        if i in skip:
            print("Skipped", i)
            continue
        latencies, stat = parse_only_client_latencies(d, num_clients, ramp_up, ramp_down)
        latencies = [l / 1000.0 for l in latencies]
        lat_p99 = quantiles(latencies, n=100)[97]
        lat_min = min(latencies)

        if lat_p99 > max_p99:
            max_p99 = lat_p99

        if lat_min < min_min:
            min_min = lat_min

        plt.plot(stat[0] / 1000.0, stat[1], label=legend[i])

    plt.xlim(min_min, max_p99)
    plt.ylim(0, 1)
    plt.legend()

    plt.xlabel("Latency (ms)")
    plt.ylabel("CDF")

    plt.savefig(out, bbox_inches="tight")


@click.command()
@click.option(
    "--path", required=True,
    type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
@click.option(
    "--start", required=True,
    type=click.STRING
)
@click.option(
    "--end", required=True,
    type=click.STRING
)
@click.option(
    "-c", "--num_clients", required=True,
    type=click.INT
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
    "--legend", multiple=True, required=True,
    type=click.STRING
)
@click.option(
    "--skip", multiple=True,
    default=[],
    type=click.INT
)
def main(path, start, end, num_clients, ramp_up, ramp_down, out, legend, skip):
    plot_all_latency_cdfs(path, start, end, num_clients, ramp_up, ramp_down, out, legend, skip)

    
if __name__ == "__main__":
    main()