
from copy import deepcopy
from dataclasses import dataclass
import os
from typing import Callable, Dict, List

from experiments import Experiment
from collections import defaultdict
import re
from dateutil.parser import isoparse
import datetime
import numpy as np
from pprint import pprint
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

    def process_experiment(self, experiment, ramp_up, ramp_down, byz) -> Stats:
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
            latencies = [float('inf')]
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

        return Stats(
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


    def tput_latency_sweep(self):
        '''
        Considers each sub-experiment in a group as a separate data point on a line graph
        and plots in the order of the experiment seq num.
        Each group is a separate line on the graph and len(legends) == len(experiment_groups)
        This expects PirateShip style logging formats.

        Legend guide:
        - <name>: Plot throughput vs latency for crash commit only
        - <name>+byz: Plot byz commit latency separately, new line for <name>-byz
        - <name>+onlybyz: Plot only byz commit latency.
        '''

        # Parse args
        ramp_up = self.kwargs.get('ramp_up', 0)
        ramp_down = self.kwargs.get('ramp_down', 0)
        legends = self.kwargs.get('legends', [])
        output = self.kwargs.get('output', 'plot.png')
        output = os.path.join(self.workdir, output)
        print(ramp_up, ramp_down, legends, output)
        assert len(legends) == len(self.experiment_groups)

        final_legends = []
        all_stats = []


        # Find parsing log files for each group
        for experiment_group_num, (group_name, experiments) in enumerate(self.experiment_groups.items()):
            print("========", group_name, "========")
            experiments.sort(key=lambda x: x.seq_num)
            final_stats = []
            if "+byz" in legends[experiment_group_num]:
                needs_byz = True
                needs_crash = True
                _legend = legends[experiment_group_num].replace("+byz", "")
                final_legends.extend([_legend, f"{_legend}-byz"])
            elif "+onlybyz" in legends[experiment_group_num]:
                needs_byz = True
                needs_crash = False
                _legend = legends[experiment_group_num].replace("+onlybyz", "")
                final_legends.append(_legend)
            else:
                needs_byz = False
                needs_crash = True
                final_legends.append(legends[experiment_group_num])
            
            if needs_crash:
                for experiment in experiments:
                    stats = self.process_experiment(experiment, ramp_up, ramp_down, byz=False)
                    final_stats.append(stats)


            if needs_byz:
                for experiment in experiments:
                    stats = self.process_experiment(experiment, ramp_up, ramp_down, byz=True)
                    final_stats.append(stats)

            all_stats.append(final_stats)

        
        plot_dict = {x[0]: x[1] for x in zip(final_legends, all_stats)}
        pprint(plot_dict)

        # Plot
        for legend, stat_list in plot_dict.items():
            tputs = [stat.mean_tput for stat in stat_list]
            latencies = [stat.mean_latency for stat in stat_list]
            plt.plot(tputs, latencies, label=legend)

        plt.xlabel("Throughput (tx/s)")
        plt.ylabel("Latency (us)")
        plt.grid()
        plt.legend()
        plt.savefig(output)   


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