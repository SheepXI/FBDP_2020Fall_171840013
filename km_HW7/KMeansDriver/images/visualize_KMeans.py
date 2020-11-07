import matplotlib.pyplot as plt
import numpy as np

plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

filename = '../km_HW7/KMeansDriver/result/K3T4.txt'

k = 3
clusters = [[] for i in range(k)]

with open(filename, 'r') as f:
    lines = f.readlines()
    for line in lines:
        clusters[0].append(line[-2])
        line = line[:-3]
        temp = [float(s) for s in line.split(',')]
        clusters[1].append(temp[0])
        clusters[2].append(temp[1])

plt.xlabel('X')
plt.ylabel('Y')
colors = ['#6664FF', '#66FF69', '#FFB566']
area = np.pi*4**2

for i in range(len(clusters[0])):
    num = int(clusters[0][i])
    plt.scatter(clusters[1][i], clusters[2][i], s=area, c=colors[num-1], alpha=0.4)

plt.show()
