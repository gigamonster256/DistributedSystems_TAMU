#!/usr/bin/env python3

import matplotlib.pyplot as plt

def graph(filepath, title, fig_filename, x_label="Hour", y_label="Count"):
    with open(filepath, 'r') as f:
        lines = f.readlines()
        points = {int(time): int(count) for time, count in [line.split() for line in lines]}
        plt.bar(points.keys(), points.values())
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.savefig(fig_filename)
        plt.cla()

if __name__ == '__main__':
    graph("output_time/part-r-00000", "Hour vs. Count", "hour_vs_count.png")
    graph("output_sleep/part-r-00000", "Hour vs. Sleep mentions", "hour_vs_sleep.png")
