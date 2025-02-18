
from copy import deepcopy
from dataclasses import dataclass
import os
import pickle
from typing import Callable, Dict, List

from experiments import Experiment
from collections import defaultdict
import re
from dateutil.parser import isoparse
import datetime
import numpy as np
from pprint import pprint
import matplotlib
import matplotlib.pyplot as plt

# Log format follows the log4rs config.
# Capture the time from the 3rd []

# Sample log: [INFO][pft::execution::engines::logger][2024-08-06T10:28:13.926997933+00:00] fork.last = 2172, fork.last_qc = 2169, commit_index = 2171, byz_commit_index = 2166, pending_acks = 200, pending_qcs = 1 num_crash_committed_txs = 100, num_byz_committed_txs = 100, fork.last_hash = b7da989badce213929ab457e5301b587593e0781e081ba7261d57cd7778e1b7b, total_client_request = 388706, view = 1, view_is_stable = true, i_am_leader: true
node_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] fork\.last = ([0-9]+), fork\.last_qc = ([0-9]+), commit_index = ([0-9]+), byz_commit_index = ([0-9]+), pending_acks = ([0-9]+), pending_qcs = ([0-9]+) num_crash_committed_txs = ([0-9]+), num_byz_committed_txs = ([0-9]+), fork\.last_hash = (.+), total_client_request = ([0-9]+), view = ([0-9]+), view_is_stable = (.+), i_am_leader\: (.+)")
node_rgx2 = re.compile(r"\[INFO\]\[.*\]\[(.*)\] num_reads = ([0-9]+)")

# Sample log: [INFO][client][2024-08-06T10:28:12.352816849+00:00] Client Id: 264, Msg Id: 224, Block num: 1000, Tx num: 145, Latency: 4073 us, Current Leader: node1
client_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Client Id\: ([0-9]+), Msg Id\: ([0-9]+), Block num\: ([0-9]+), Tx num\: ([0-9]+), Latency\: ([.0-9]+) us, Current Leader\: (.+)")
client_byz_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Client Id\: ([0-9]+), Block num\: ([0-9]+), Tx num\: ([0-9]+), Byz Latency\: ([.0-9]+) us")


def process_tput(points, ramp_up, ramp_down, tputs, tputs_unbatched, byz=False, read_points=[[]]):
    points = [
        (
            isoparse(a[0]),    # ISO format is used in run_remote
            int(a[1]),         # fork.last
            int(a[2]),         # fork.last_qc
            int(a[3]),         # commit_index
            int(a[4]),         # byz_commit_index
            int(a[5]),         # pending_acks
            int(a[6]),         # pending_qcs
            int(a[7]),         # num_crash_txs,
            int(a[8]),         # num_byz_txs,
            a[9],              # fork.last_hash,
            int(a[10]),         # total_client_request
            int(a[11]),        # view
            a[12] == "true",   # view_is_stable
            a[13] == "true"    # i_am_leader
        )
        for a in points
    ]

    read_points = [
        [
            (
                isoparse(a[0]),
                int(a[1])
            )
            for a in _rp
        ]
        for _rp in read_points
    ]

    # Filter points, only keep if after ramp_up time and before ramp_down time

    start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
    end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

    points = [p for p in points if p[0] >= start_time and p[0] <= end_time]
    read_points = [
        [p for p in _rp if p[0] >= start_time and p[0] <= end_time]
        for _rp in read_points
    ]

    total_runtime = (points[-1][0] - points[0][0]).total_seconds()
    if byz:
        total_commit = points[-1][4] - points[0][4]
        total_tx = points[-1][8] - points[0][8]
    else:
        total_commit = points[-1][3] - points[0][3]
        total_tx = points[-1][7] - points[0][7]
    
    total_reads = 0

    for _rp in read_points:
        if len(_rp) > 0:
            total_reads += _rp[-1][1] - _rp[0][1]
    
    total_tx += total_reads

    print(total_commit, total_tx, total_runtime)

    tputs.append(total_tx / total_runtime)
    tputs_unbatched.append(total_commit / total_runtime)


def process_latencies(points, ramp_up, ramp_down, latencies):
    if len(points[0]) == 7:
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
    else:
        # Byz logs have 2 entries less
        points = [
            (
                isoparse(a[0]),      # ISO format is used in run_remote
                int(a[1]),           # Client Id
                0,                   # Dummy Msg Id
                int(a[2]),           # Block num
                int(a[3]),           # Tx num
                float(a[4]),         # Latency us
                "node0"              # Dummy Current Leader
            )
            for a in points
        ]
    total_n = len(points)

    # Filter points, only keep if after ramp_up time and before ramp_down time

    start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
    end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

    points = [p for p in points if p[0] >= start_time and p[0] <= end_time]

    latencies.extend([p[5] for p in points])


@dataclass
class Stats:
    mean_tput: float
    stdev_tput: float
    mean_tput_unbatched: float
    stdev_tput_unbatched: float
    latency_prob_dist: List[float]
    mean_latency: float
    median_latency: float
    p25_latency: float
    p75_latency: float
    p99_latency: float
    max_latency: float
    min_latency: float
    stdev_latency: float


@dataclass
class Result:
    name: str
    plotter_func: Callable
    experiment_groups: Dict[str, List[Experiment]]
    workdir: str
    kwargs: dict


    def default_output(self):
        print(f"Default output method: {self}")

    def parse_node_logs(self, log_dir, node_log_names, ramp_up, ramp_down, tputs, tputs_unbatched, byz=False):
        points = []
        read_points = []
        with open(os.path.join(log_dir, node_log_names[0]), "r") as f:
            for line in f.readlines():
                captures = node_rgx.findall(line)
                if len(captures) == 1:
                    points.append(captures[0])

        num_nodes = len(node_log_names)
        for node_num in range(num_nodes):
            _rp = []
            with open(os.path.join(log_dir, node_log_names[node_num]), "r") as f:
                for line in f.readlines():
                    captures = node_rgx2.findall(line)
                    if len(captures) == 1:
                        _rp.append(captures[0])
            read_points.append(_rp)

        try:
            process_tput(points, ramp_up, ramp_down, tputs, tputs_unbatched, byz, read_points)
        except:
            pass
    

    def parse_client_logs(self, log_dir, client_log_names, ramp_up, ramp_down, latencies, byz=False):
        points = []
        for log_name in client_log_names:
            fname = os.path.join(log_dir, log_name)
            try:
                with open(fname, "r") as f:
                    for line in f.readlines():
                        if byz:
                            captures = client_byz_rgx.findall(line)
                        else:
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

    def process_experiment(self, experiment, ramp_up, ramp_down, byz, tput_scale=1000.0, latency_scale=1000.0) -> Stats | None:
        tputs = []
        tputs_unbatched = []
        latencies = []
        for repeat_num in range(experiment.repeats):
            log_dir = os.path.join(experiment.local_workdir, "logs", str(repeat_num))
            # Find the first node log file and all client log files in log_dir
            node_log_files = list(sorted([f for f in os.listdir(log_dir) if f.startswith("node") and f.endswith(".log")]))
            client_log_files = [f for f in os.listdir(log_dir) if f.startswith("client") and f.endswith(".log")]

            self.parse_node_logs(log_dir, node_log_files, ramp_up, ramp_down, tputs, tputs_unbatched, byz=byz)
            self.parse_client_logs(log_dir, client_log_files, ramp_up, ramp_down, latencies, byz=byz)
        print(len(tputs), len(tputs_unbatched), len(latencies))
        if len(latencies) == 0:
            return None
        latency_prob_dist = np.array(latencies)
        latency_prob_dist.sort()
        # latency_prob_dist = latency_prob_dist.clip(latency_prob_dist[0], quantiles(latencies, n=100)[98])
        p = 1. * np.arange(len(latency_prob_dist)) / (len(latency_prob_dist) - 1)
        try:
            stdev_tput = np.std(tputs)
        except:
            stdev_tput = 0
        
        try:
            stdev_tput_unbatched = np.std(tputs_unbatched)
        except:
            stdev_tput_unbatched = 0
        
        try:
            stdev_latency = np.std(latencies)
        except:
            stdev_latency = 0

        mean_latency = np.mean(latencies)

        try:
            median_latency = np.median(latencies)
            p25_latency=np.percentile(latencies, 25),
            p75_latency=np.percentile(latencies, 75),
            p99_latency=np.percentile(latencies, 99),
        except:
            median_latency = mean_latency
            p25_latency = mean_latency
            p75_latency = mean_latency
            p99_latency = mean_latency

        ret = Stats(
            mean_tput=np.mean(tputs),
            stdev_tput=stdev_tput,
            mean_tput_unbatched=np.mean(tputs_unbatched),
            stdev_tput_unbatched=stdev_tput_unbatched,
            latency_prob_dist=latency_prob_dist,
            mean_latency=mean_latency,
            median_latency=median_latency,
            p25_latency=p25_latency,
            p75_latency=p75_latency,
            p99_latency=p99_latency,
            max_latency=np.max(latencies),
            min_latency=np.min(latencies),
            stdev_latency=stdev_latency
        )

        for k, v in ret.__dict__.items():
            if "tput" in k and isinstance(v, float):
                ret.__dict__[k] /= tput_scale
            if "latency" in k and isinstance(v, float):
                ret.__dict__[k] /= latency_scale

        return ret


    def tput_latency_sweep_parse(self, ramp_up, ramp_down, legends) -> Dict[str, List[Stats]]:
        plot_dict = {}

        # Find parsing log files for each group
        for group_name, experiments in self.experiment_groups.items():
            print("========", group_name, "========")
            experiments.sort(key=lambda x: x.seq_num)
            crash_legend = None
            byz_legend = None
            legend = legends.get(group_name, None)
            if legend is None:
                print("\x1b[31;1mNo legend found for", group_name, ". Skipping...\x1b[0m")
                continue
            if "+byz" in legend:
                needs_byz = True
                needs_crash = True
                names = legend.split("+")
                _legend = names[0]
                crash_legend = _legend[:]
                if len(names) == 2:
                    byz_legend = _legend[:] + "-byz"
                else:
                    byz_legend = names[1]
            elif "+onlybyz" in legend:
                needs_byz = True
                needs_crash = False
                _legend = legend.replace("+onlybyz", "")
                byz_legend = _legend[:]
            else:
                needs_byz = False
                needs_crash = True
                crash_legend = legend[:]
            
            if needs_crash:
                final_stats = []

                for experiment in experiments:
                    stats = self.process_experiment(experiment, ramp_up, ramp_down, byz=False)
                    if stats is not None:
                        final_stats.append(stats)
                    else:
                        print("\x1b[31;1mSkipping experiment", experiment.name, "for crash commit\x1b[0m")

                plot_dict[crash_legend] = final_stats
                

            if needs_byz:
                final_stats = []

                for experiment in experiments:
                    stats = self.process_experiment(experiment, ramp_up, ramp_down, byz=True)
                    if stats is not None:
                        final_stats.append(stats)
                    else:
                        print("\x1b[31;1mSkipping experiment", experiment.name, "for byz commit\x1b[0m")

                plot_dict[byz_legend] = final_stats

        pprint(plot_dict)
        return plot_dict


    def tput_latency_sweep_plot(self, plot_dict: Dict[str, List[Stats]], output: str | None):
        # Find how many subfigures we need.

        bounding_boxes = {
            k: [
                min([stat.mean_tput for stat in v]),    # Xmin
                max([stat.mean_tput for stat in v]),    # Xmax
                min([stat.mean_latency for stat in v]), # Ymin
                max([stat.mean_latency for stat in v]), # Ymax
            ]
            for k, v in plot_dict.items()
        }


        # Leave padding around bounding boxes
        for k, v in bounding_boxes.items():
            v[0] -= 0.1 * v[0]
            v[1] += 0.1 * v[1]
            v[2] -= 0.1 * v[2]
            v[3] += 0.1 * v[3]
            bounding_boxes[k] = v

        num_x_axis_breaks = 0
        num_y_axis_breaks = 0

        x_sorted_box = list(bounding_boxes.items())
        x_sorted_box.sort(key=lambda x: x[1][0])
        curr_x_cluster = [x_sorted_box[0]]
        x_clusters = []
        for i in range(1, len(x_sorted_box)):
            last_x_start = x_sorted_box[i-1][1][0]
            last_x_end = x_sorted_box[i-1][1][1]
            this_x_start = x_sorted_box[i][1][0]
            this_x_end = x_sorted_box[i][1][1]

            total_x_range = this_x_end - last_x_start
            gap = this_x_start - last_x_end

            if this_x_start > last_x_end and gap > 0.2 * total_x_range:
                # This is a heuristic to determine if we need to break the x-axis
                num_x_axis_breaks += 1
                x_clusters.append(deepcopy(curr_x_cluster))
                curr_x_cluster = [x_sorted_box[i]]
            else:
                curr_x_cluster.append(x_sorted_box[i])
        
        if len(curr_x_cluster) > 0:
            x_clusters.append(deepcopy(curr_x_cluster))

        x_ranges = [
            (min([v[1][0] for v in cluster]), max([v[1][1] for v in cluster]))
            for cluster in x_clusters
        ]


        y_sorted_box = list(bounding_boxes.items())
        y_sorted_box.sort(key=lambda x: x[1][2])
        curr_y_cluster = [y_sorted_box[0]]
        y_clusters = []
        for i in range(1, len(y_sorted_box)):
            last_y_start = y_sorted_box[i-1][1][2]
            last_y_end = y_sorted_box[i-1][1][3]
            this_y_start = y_sorted_box[i][1][2]
            this_y_end = y_sorted_box[i][1][3]

            total_y_range = this_y_end - last_y_start
            gap = this_y_start - last_y_end

            if this_y_start > last_y_end and gap > 0.2 * total_y_range:
                # This is a heuristic to determine if we need to break the y-axis
                num_y_axis_breaks += 1
                y_clusters.append(deepcopy(curr_y_cluster))
                curr_y_cluster = [y_sorted_box[i]]
            else:
                curr_y_cluster.append(y_sorted_box[i])
        if len(curr_y_cluster) > 0:
            y_clusters.append(deepcopy(curr_y_cluster))

        y_ranges = [
            (min([v[1][2] for v in cluster]), max([v[1][3] for v in cluster]))
            for cluster in y_clusters
        ]

        total_x_axes = num_x_axis_breaks + 1
        total_y_axes = num_y_axis_breaks + 1
        print(total_x_axes, total_y_axes, x_clusters, y_clusters)

        assert(total_x_axes == len(x_ranges))
        assert(total_y_axes == len(y_ranges))

        is_1d = total_x_axes == 1 or total_y_axes == 1

        font = self.kwargs.get('font', {
            'size'   : 65
        })
        matplotlib.rc('font', **font)
        matplotlib.rc("axes.formatter", limits=(-99, 99))

        fig, axes = plt.subplots(
            total_y_axes, total_x_axes,
            figsize=(10 * total_x_axes, 6 * total_y_axes),
            sharex='col', sharey='row',
        )


        fig.subplots_adjust(hspace=0.1, wspace=0.1)

        num_lines = len(plot_dict)
        colors = self.kwargs.get('colors', ['b', 'g', 'r', 'c', 'm', 'y', 'k'])
        markers = self.kwargs.get('markers', ['o', 's', 'D', '^', 'v', 'p', 'P', '*', 'X', 'H'])
        while len(colors) < num_lines:
            colors += colors
        while len(markers) < num_lines:
            markers += markers

        
        legends_ncols = self.kwargs.get('legends_ncols', len(plot_dict))
        if legends_ncols > 5:
            legends_ncols = 5

    
        for ax in axes:
            ax.grid()
            for i, (legend, stat_list) in enumerate(plot_dict.items()):
                tputs = [stat.mean_tput for stat in stat_list]
                latencies = [stat.mean_latency for stat in stat_list]
                ax.plot(tputs, latencies, label=legend, color=colors[i], marker=markers[i], mew=6, ms=12, linewidth=6)

        if is_1d:
            axes[0].set_xlabel("Throughput (k req/s)")
            axes[0].set_ylabel("Latency (ms)")
            axes[0].yaxis.set_label_coords(0.04, 0.5, transform=fig.transFigure)
            axes[0].xaxis.set_label_coords(0.5, 0.05, transform=fig.transFigure)
            axes[0].legend(loc='upper center', bbox_to_anchor=(0.5, 1.45), ncol=legends_ncols, fontsize=55, columnspacing=1)
        else:
            axes[0, 0].set_xlabel("Throughput (k req/s)")
            axes[0, 0].set_ylabel("Latency (ms)")
            axes[0, 0].yaxis.set_label_coords(0.04, 0.5, transform=fig.transFigure)
            axes[0, 0].xaxis.set_label_coords(0.5, 0.05, transform=fig.transFigure)
            axes[0, 0].legend(loc='upper center', bbox_to_anchor=(0.5, 1.45), ncol=legends_ncols, fontsize=55, columnspacing=1)

        for ax in axes:
            ax.spines.bottom.set_visible(False)
            ax.spines.top.set_visible(False)
            ax.xaxis.tick_top()
            ax.xaxis.tick_bottom()
            ax.tick_params(labeltop=False, labelbottom=False)
            ax.spines.right.set_visible(False)
            ax.spines.left.set_visible(False)
            ax.yaxis.tick_left()
            ax.yaxis.tick_right()
            ax.tick_params(labelright=False, labelleft=False)

        # Make bottom spine visible for the last row
        for x in range(total_x_axes):
            ycoord = -1
            if is_1d and total_x_axes > 1:
                # ----------
                axes[x].spines.bottom.set_visible(True)
                axes[x].tick_params(labelbottom=True)
            elif is_1d:
                # |
                # |
                # |
                axes[ycoord].spines.bottom.set_visible(True)
                axes[ycoord].tick_params(labelbottom=True)
            else:
                # |||
                # |||
                axes[ycoord, x].spines.bottom.set_visible(True)
                axes[ycoord, x].tick_params(labelbottom=True)

        # Make top spine visible for the first row
        for x in range(total_x_axes):
            ycoord = 0
            if is_1d and total_x_axes > 1:
                # ----------
                axes[x].spines.top.set_visible(True)
                # axes[x].tick_params(labeltop=True)
            elif is_1d:
                # |
                # |
                # |
                axes[ycoord].spines.top.set_visible(True)
                # axes[ycoord].tick_params(labeltop=True)
            else:
                # |||
                # |||
                axes[ycoord, x].spines.top.set_visible(True)
                # axes[ycoord, x].tick_params(labeltop=True)

        # Make left spine visible for the first col
        for y in range(total_y_axes):
            if is_1d and total_y_axes > 1:
                # |
                # |
                # |
                axes[y].spines.left.set_visible(True)
                axes[y].tick_params(labelleft=True)
            elif is_1d:
                # ----------
                axes[0].spines.left.set_visible(True)
                axes[0].tick_params(labelleft=True)
            else:
                # |||
                # |||
                axes[y, 0].spines.left.set_visible(True)
                axes[y, 0].tick_params(labelleft=True)

        # Make right spine visible for the last col
        for y in range(total_y_axes):
            xcoord = -1
            if is_1d and total_y_axes > 1:
                # |
                # |
                # |
                axes[y].spines.right.set_visible(True)
                # axes[y].tick_params(labelright=True)
            elif is_1d:
                # ----------
                axes[xcoord].spines.right.set_visible(True)
                # axes[xcoord].tick_params(labelright=True)
            else:
                # |||
                # |||
                axes[y, xcoord].spines.right.set_visible(True)
                # axes[y, xcoord].tick_params(labelright=True)

        
        d = .5  # proportion of vertical to horizontal extent of the slanted line
        kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
                    linestyle="none", color='k', mec='k', mew=1, clip_on=False)
        if is_1d and total_x_axes > 1:
            # ----------
            # Put a \ mark on the right of x-axis for all except the last one
            for x in range(total_x_axes - 1):
                axes[x].plot([1, 1], [1, 0], transform=axes[x].transAxes, **kwargs)
            
            # Put a \ mark on the left of x-axis for all except the first one
            for x in range(1, total_x_axes):
                axes[x].plot([0, 0], [1, 0], transform=axes[x].transAxes, **kwargs)

        elif is_1d:
            # |
            # |
            # |
            # Put a \ mark on the top of y-axis for all except the first one
            for y in range(1, total_y_axes):
                axes[y].plot([0, 1], [1, 1], transform=axes[y].transAxes, **kwargs)

            # Put a \ mark on the bottom of y-axis for all except the last one
            for y in range(total_y_axes - 1):
                axes[y].plot([0, 1], [0, 0], transform=axes[y].transAxes, **kwargs)

        else:
            # |||
            # |||
            for x in range(total_x_axes):
                # Put a \ mark on the top of y-axis for all except the first one
                for y in range(1, total_y_axes):
                    axes[y, x].plot([0, 1], [1, 1], transform=axes[y, x].transAxes, **kwargs)

                # Put a \ mark on the bottom of y-axis for all except the last one
                for y in range(total_y_axes - 1):
                    axes[y, x].plot([0, 1], [0, 0], transform=axes[y, x].transAxes, **kwargs)

            for y in range(total_y_axes):
                # Put a \ mark on the right of x-axis for all except the last one
                for x in range(total_x_axes - 1):
                    axes[x].plot([1, 1], [1, 0], transform=axes[x].transAxes, **kwargs)
                
                # Put a \ mark on the left of x-axis for all except the first one
                for x in range(1, total_x_axes):
                    axes[x].plot([0, 0], [1, 0], transform=axes[x].transAxes, **kwargs)
            


        for x in range(total_x_axes):
            for y in range(total_y_axes):
                xlim = x_ranges[x]
                ylim = y_ranges[y]
                ycoord = total_y_axes - y - 1
                xcoord = x

                if total_x_axes == 1:
                    axes[ycoord].set_xlim(xlim)
                    axes[ycoord].set_ylim(ylim)
                elif total_y_axes == 1:
                    axes[xcoord].set_xlim(xlim)
                    axes[xcoord].set_ylim(ylim)
                else:
                    axes[ycoord, xcoord].set_xlim(xlim)
                    axes[ycoord, xcoord].set_ylim(ylim)


        plt.gcf().set_size_inches(
            self.kwargs.get("output_width", 30),
            self.kwargs.get("output_height", 12)
        )

        if output is not None:
            output = os.path.join(self.workdir, output)
            plt.savefig(output, bbox_inches="tight")
        else:
            plt.show()




    def tput_latency_sweep(self):
        '''
        Considers each sub-experiment in a group as a separate data point on a line graph
        and plots in the order of the experiment seq num.
        Each group is a separate line on the graph and len(legends) == len(experiment_groups)
        This expects PirateShip style logging formats.

        Legend guide:
        - <name>: Plot throughput vs latency for crash commit only
        - <name>+byz: Plot byz commit latency separately, new line for <name>-byz
        - <name_1>+<name_2>+byz: Plot byz commit latency separately with legend <name2>
        - <name>+onlybyz: Plot only byz commit latency.
        '''

        # Parse args
        ramp_up = self.kwargs.get('ramp_up', 0)
        ramp_down = self.kwargs.get('ramp_down', 0)
        legends = self.kwargs.get('legends', {})
        force_parse = self.kwargs.get('force_parse', False)

        # Try retreive plot dict from cache
        try:
            if force_parse:
                raise Exception("Force parse")

            with open(os.path.join(self.workdir, "plot_dict.pkl"), "rb") as f:
                plot_dict = pickle.load(f)
        except:
            plot_dict = self.tput_latency_sweep_parse(ramp_up, ramp_down, legends)

        # Save plot dict
        with open(os.path.join(self.workdir, "plot_dict.pkl"), "wb") as f:
            pickle.dump(plot_dict, f)

        output = self.kwargs.get('output', None)
        self.tput_latency_sweep_plot(plot_dict, output)


    def __init__(self, name, workdir, fn_name, experiments, kwargs):
        self.name = name

        self.workdir = os.path.join(workdir, "results", self.name)
        os.makedirs(self.workdir, exist_ok=True)

        self.plotter_func = getattr(self, fn_name, self.default_output)
        self.experiments = experiments[:]
        self.experiment_groups = defaultdict(list)
        for experiment in self.experiments:
            self.experiment_groups[experiment.group_name].append(experiment)
        
        self.kwargs = deepcopy(kwargs)

    def output(self):
        self.plotter_func()