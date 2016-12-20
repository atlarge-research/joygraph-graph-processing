import matplotlib.pyplot as plt
import numpy as np

numSupersteps = 7
yAverageProcSpeed = [40316.333333333336,38390.333333333336,29243.666666666668,17818.666666666668,1807.0,933.6666666666666,201.0]
yElasticOverhead = [80902.0,71493.33333333333,63510.0,0.0,71340.33333333333,136511.33333333334,0.0]
yProcSpeedError = [1345.993065856334,641.2880268127055,1644.7326023805006,290.9112121134786,157.87653403846943,22.501851775650227,16.3707055437449]
yElasticOverheadError = [5493.870857601223,3844.5044327368555,5633.325927016827,0.0,3998.50801341367,897.1322830738693,0.0]

yProcSpeedError = list(map(lambda x: x / 1000, yProcSpeedError))
yElasticOverheadError = list(map(lambda x: x / 1000, yElasticOverheadError))
yAverageProcSpeed = list(map(lambda x: x / 1000, yAverageProcSpeed))
yElasticOverhead = list(map(lambda x: x / 1000, yElasticOverhead))

barWidth = 0.35
steps = np.arange(0, numSupersteps, 1)

fig = plt.figure()
barChart = fig.add_axes((0.1, 0.1, 0.8, 0.8))
barChart.set_xlim([-0.1, max(steps) + 0.5])
p1 = barChart.bar(steps, yAverageProcSpeed, barWidth, color='r', yerr = yProcSpeedError)
p2 = barChart.bar(steps, yElasticOverhead, barWidth, bottom=yAverageProcSpeed, yerr = yElasticOverheadError)
barChart.set_xticks(steps + barWidth/2.)
barChart.set_xticklabels(steps)
barChart.set_xlabel('supersteps')
barChart.set_ylabel('time (s)')
barChart.legend((p1[0], p2[0]), ('t_proc', 't_elastic'), loc = 0)

plt.show()
      