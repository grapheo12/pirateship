from statistics import mean, median
import sys

inp = []
for line in sys.stdin:
    inp.append(int(line.strip()))


print("Mean:", mean(inp), "Median:", median(inp), "Min:", min(inp), "Max:", max(inp))