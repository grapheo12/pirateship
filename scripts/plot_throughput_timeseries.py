# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

import datetime
from plot_utils import node_rgx, isoparse
import matplotlib.pyplot as plt
import numpy as np
import re
import click

# Sample: [WARN][pft::consensus::steady_state][2024-11-12T14:22:57.240051-08:00] Equivocated on block 5000, Partitions: ["node2"] ["node3", "node4"]
equivocate_rgx = re.compile(r"\[WARN\]\[.*\]\[(.*)\] Equivocated on block .*")

# Sample: [INFO][pft::consensus::handler][2024-11-12T14:23:04.049113-08:00] Processing view change for view 2 from node3
revolt_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Processing view change for view .* from .*")

# Sample: [INFO][pft::consensus::handler][2024-11-12T14:23:04.049113-08:00] Moved to new view 2 with leader node2
moved_view_rgx = re.compile(r"\[WARN\]\[.*\]\[(.*)\] Moved to new view .* with leader.*")

stable_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] View stabilised.*")
stable_rgx2 = re.compile(r"\[INFO\]\[.*\]\[(.*)\]View fast forwarded to .* stable\? true")

# Sample: [INFO][controller][2024-11-14T22:02:44.250572996+00:00] ProtoTransactionReceipt
controller_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] ProtoTransactionReceipt.*")



def plot_throughput_timeseries(infile, outfile, ramp_up=5, ramp_down=5):
    points = []
    first_equivocate = None
    first_revolt = None
    moved_view = None
    view_stabilized = None

    with open(infile, "r") as f:
        for line in f.readlines():
            captures = node_rgx.findall(line)
            # print(captures)
            if len(captures) == 1:
                points.append(captures[0])

            if first_equivocate is None:
                eq_cp = equivocate_rgx.findall(line)
                if len(eq_cp) == 1:
                    first_equivocate = isoparse(eq_cp[0])

            else: # Need to check what happens AFTER equivocation
                if first_revolt is None:
                    r_cp = revolt_rgx.findall(line)
                    if len(r_cp) == 1:
                        first_revolt = isoparse(r_cp[0])
                        
                if moved_view is None:
                    nl_cp = moved_view_rgx.findall(line)
                    if len(nl_cp) == 1:
                        moved_view = isoparse(nl_cp[0])

                if view_stabilized is None:
                    st_cp = stable_rgx.findall(line)
                    if len(st_cp) == 1:
                        view_stabilized = isoparse(st_cp[0])

                if view_stabilized is None:
                    st_cp = stable_rgx2.findall(line)
                    if len(st_cp) == 1:
                        view_stabilized = isoparse(st_cp[0])
                

    points = [
        (
            isoparse(a[0]),    # ISO format is used in run_remote
            int(a[3]),         # commit_index
            int(a[4]),         # byz_commit_index
        )
        for a in points
    ]

    diff_points = [
        (
            (a[0] - points[0][0]).total_seconds(),
            (a[1] - points[i - 1][1]) / (a[0] - points[i - 1][0]).total_seconds(),
            (a[2] - points[i - 1][2]) / (a[0] - points[i - 1][0]).total_seconds()
        )
        for i, a in enumerate(points) if i > 0
    ]

    try:
        first_equivocate = (first_equivocate - points[0][0]).total_seconds()
    except:
        first_equivocate = None
    
    try:
        first_revolt = (first_revolt - points[0][0]).total_seconds()
    except:
        first_revolt = None

    try:
        moved_view = (moved_view - points[0][0]).total_seconds()
    except:
        moved_view = None

    try:
        view_stabilized = (view_stabilized - points[0][0]).total_seconds()
    except:
        view_stabilized = None


    plt.plot(
        np.array([x[0] for x in diff_points]),
        np.array([x[1] for x in diff_points]),
        label="CI Tput"
    )

    plt.plot(
        np.array([x[0] for x in diff_points]),
        np.array([x[2] for x in diff_points]),
        label="BCI Tput"
    )

    if not(first_equivocate is None):
        plt.axvline(x=first_equivocate, label="Equivocation start", color='red', linestyle='dashed')
    
    if not(first_revolt is None) and first_revolt >= first_equivocate:
        plt.axvline(x=first_revolt, label="Followers start revolting", color='magenta', linestyle='dashed')
    
    if not(moved_view is None) and not(view_stabilized is None) and moved_view <= view_stabilized:
        plt.axvline(x=moved_view, label="Leader step down", color='black', linestyle='dashed')

    if not(view_stabilized is None) and view_stabilized >= first_equivocate:
        plt.axvline(x=view_stabilized, label="New view stabilized", color='green', linestyle='dashed')

    plt.yscale("symlog")
    plt.grid()
    plt.ylabel("Throughput (blocks/s)")
    plt.xlabel("Time elapsed (s)")
    plt.xlim((ramp_up, diff_points[-1][0] - ramp_down))
    
    # Legend to the bottom and right
    plt.legend(loc='lower right')
    # Wide and short figure
    plt.gcf().set_size_inches(20, 12)

    # Save the figure, tight layout to prevent cropping
    plt.savefig(outfile, bbox_inches='tight')



def plot_throughput_timeseries_multi(infiles, legends, outfile, ramp_up=5, ramp_down=5, cmd_files=[]):
    assert(len(infiles) == len(legends))

    for i, infile in enumerate(infiles):
        points = []

        with open(infile, "r") as f:
            for line in f.readlines():
                captures = node_rgx.findall(line)
                # print(captures)
                if len(captures) == 1:
                    points.append(captures[0])

        points = [
            (
                isoparse(a[0]),    # ISO format is used in run_remote
                int(a[3]),         # commit_index
                int(a[4]),         # byz_commit_index
            )
            for a in points
        ]

        diff_points = [
            (
                (a[0] - points[0][0]).total_seconds(),
                (a[1] - points[i - 10][1]) / (a[0] - points[i - 10][0]).total_seconds(),
                (a[2] - points[i - 10][2]) / (a[0] - points[i - 10][0]).total_seconds()
            )
            for i, a in enumerate(points) if i >= 10
        ]

        plt.plot(
            np.array([x[0] for x in diff_points]),
            np.array([x[2] for x in diff_points]),
            label=legends[i]
        )

    plt.yscale("symlog")
    plt.grid()
    plt.ylabel("Byzantine Commit Throughput (blocks/s)")
    plt.xlabel("Time elapsed (s)")
    plt.xlim((ramp_up, diff_points[-1][0] - ramp_down))
    plt.ylim(0)
    
    # Legend to the bottom and right
    plt.legend(loc='lower right')

    for i, path in enumerate(cmd_files):
        with open(path, "r") as f:
            for line in f.readlines():
                captures = controller_rgx.findall(line)
                # print(captures)
                if len(captures) == 1:
                    x = isoparse(captures[0])
                    x = (x - points[0][0]).total_seconds()
                    plt.axvline(x=x, label=f"cmd{i}", linestyle='dashed', color="red")

    # Wide and short figure
    plt.gcf().set_size_inches(20, 12)

    # Save the figure, tight layout to prevent cropping
    plt.savefig(outfile, bbox_inches='tight')


@click.command()
@click.option(
    "-i", "--infile", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    help="Path to log file"
)
@click.option(
    "-o", "--outfile", required=True,
    type=click.Path(file_okay=True, resolve_path=True),
    help="Output path"
)
@click.option(
    "-up", "--ramp_up",
    default=2,
    help="Ramp up seconds to ignore in plotting",
    type=click.INT
)
@click.option(
    "-down", "--ramp_down",
    default=2,
    help="Ramp down seconds to ignore in plotting",
    type=click.INT
)
def main(infile, outfile, ramp_up, ramp_down):
    plot_throughput_timeseries(infile, outfile, ramp_up, ramp_down)

if __name__ == "__main__":
    main()