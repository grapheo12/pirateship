from pprint import pprint
import click
from plot_utils import plot_tput_vs_latency_multi, parse_log_dir_with_total_clients, plot_tput_vs_latency
import os
from dateutil.parser import isoparse

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
@click.option(
    "--legend", multiple=True,
    default=[],
    type=click.STRING
)
def main(path, start, end, repeats, num_clients, leader, ramp_up, ramp_down, out, legend):
    dir = list(os.scandir(path))
    dir = [str(p.path) for p in dir if p.is_dir()]
    start = isoparse(start)
    end = isoparse(end)
    dir = [p for p in dir if start <= isoparse(os.path.basename(p)) <= end]
    dir.sort(key=lambda x: isoparse(os.path.basename(x)))
    pprint(dir)

    print(len(dir), start, end)

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
        

