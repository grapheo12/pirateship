import datetime
from plot_utils import node_rgx, isoparse
import matplotlib.pyplot as plt
import numpy as np
import re

points = []
first_equivocate = None
first_revolt = None
moved_view = None
view_stabilized = None

# Sample: [WARN][pft::consensus::steady_state][2024-11-12T14:22:57.240051-08:00] Equivocated on block 5000, Partitions: ["node2"] ["node3", "node4"]
equivocate_rgx = re.compile(r"\[WARN\]\[.*\]\[(.*)\] Equivocated on block .*")

# Sample: [INFO][pft::consensus::handler][2024-11-12T14:23:04.049113-08:00] Processing view change for view 2 from node3
revolt_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Processing view change for view .* from .*")

# Sample: [INFO][pft::consensus::handler][2024-11-12T14:23:04.049113-08:00] Moved to new view 2 with leader node2
moved_view_rgx = re.compile(r"\[WARN\]\[.*\]\[(.*)\] Moved to new view .* with leader.*")

stable_rgx = re.compile(r"\[INFO\]\[.*\]\[(.*)\] View stabilised.*")


with open(f"logs/1.log", "r") as f:
    for line in f.readlines():
        captures = node_rgx.findall(line)
        # print(captures)
        if len(captures) == 1:
            points.append(captures[0])

        if first_equivocate is None:
            eq_cp = equivocate_rgx.findall(line)
            if len(eq_cp) == 1:
                first_equivocate = isoparse(eq_cp[0])

        if not(first_equivocate is None) and first_revolt is None:
            r_cp = revolt_rgx.findall(line)
            if len(r_cp) == 1:
                first_revolt = isoparse(r_cp[0])
                
        if not(first_equivocate is None) and not(first_revolt is None) and moved_view is None:
            nl_cp = moved_view_rgx.findall(line)
            if len(nl_cp) == 1:
                moved_view = isoparse(nl_cp[0])

        if not(first_equivocate is None) and not(first_revolt is None) and not(moved_view is None) and view_stabilized is None:
            st_cp = stable_rgx.findall(line)
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

# Filter points, only keep if after ramp_up time and before ramp_down time
ramp_up = 10
ramp_down = 5

start_time = points[0][0] + datetime.timedelta(seconds=ramp_up)
end_time = points[-1][0] - datetime.timedelta(seconds=ramp_down)

points = [p for p in points if p[0] >= start_time and p[0] <= end_time]


diff_points = [
    (
        (a[0] - points[0][0]).total_seconds(),
        (a[1] - points[i - 1][1]) / (a[0] - points[i - 1][0]).total_seconds(),
        (a[2] - points[i - 1][2]) / (a[0] - points[i - 1][0]).total_seconds()
    )
    for i, a in enumerate(points) if i > 0
]

first_equivocate = (first_equivocate - points[0][0]).total_seconds()
first_revolt = (first_revolt - points[0][0]).total_seconds()
moved_view = (moved_view - points[0][0]).total_seconds()
view_stabilized = (view_stabilized - points[0][0]).total_seconds()


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

plt.axvline(x=first_equivocate, label="Equivocation start", color='red', linestyle='dashed')
plt.axvline(x=first_revolt, label="Followers start revolting", color='magenta', linestyle='dashed')
plt.axvline(x=moved_view, label="Leader step down", color='black', linestyle='dashed')
plt.axvline(x=view_stabilized, label="New view stabilized", color='green', linestyle='dashed')
plt.yscale("symlog")
plt.grid()
plt.legend()
plt.ylabel("Throughput (blocks/s)")
plt.xlabel("Time elapsed (s)")
plt.show()