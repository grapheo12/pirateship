# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the Apache 2.0 License.

from pprint import pprint
import click
from plot_utils import parse_log_dir_with_sig_delay, plot_tput_bar_graph, plot_latency_cdf
import os
from dateutil.parser import isoparse
import matplotlib.pyplot as plt

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
def main(path, start, end, repeats, num_clients, leader, ramp_up, ramp_down, out):
    dir = list(os.scandir(path))
    dir = [str(p.path) for p in dir if p.is_dir()]
    start = isoparse(start)
    end = isoparse(end)
    dir = [p for p in dir if start <= isoparse(os.path.basename(p)) <= end]
    dir.sort(key=lambda x: isoparse(os.path.basename(x)))
    pprint(dir)

    print(len(dir), start, end)

    stats = {}
    for d in dir:
        res = parse_log_dir_with_sig_delay(d, repeats, num_clients, leader, ramp_up, ramp_down)
        stats.update(res)

    uniq_blocks = set()
    uniq_ms = set()
    for k in stats.keys():
        blk, ms = k.split()
        uniq_blocks.add(int(blk))
        uniq_ms.add(int(ms))

    if len(uniq_blocks) == 1:
        # Only retain the ms part
        stats = {
            f"{k.split()[1]} ms": v for k, v in stats.items()
        }
    elif len(uniq_ms) == 1:
        # Only retain the blocks part
        stats = {
            f"{k.split()[0]} blks": v for k, v in stats.items()
        }
    else:
        # Retain everything
        stats = {
            f"{k.split()[0]} blks, {k.split()[1]} ms": v for k, v in stats.items()
        }



    pprint(stats)
    plot_latency_cdf(stats, f"latency_cdf_{out}")
    plt.close()
    plot_tput_bar_graph(stats, f"tput_{out}")


if __name__ == "__main__":
    main()
        

