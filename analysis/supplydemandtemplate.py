import matplotlib.pyplot as plt

x1Supply = [1477594497257,1477594660207,1477594660207,1477594847678,1477594847678,1477594925871,1477594925871,1477595092841,1477595092841,1477595153256,1477595153256,1477595308236,1477595308236,1477595342730,1477595342730,1477595348375,1477595348375,1477595499986,1477595499986,1477595503682,1477595503682,1477595794118,1477595794118,1477595795512,1477595795512]
y1Supply = [10,10,10,10,12,12,12,12,14,14,14,14,17,17,17,17,17,17,5,5,5,5,3,3,3]
x2Demand = [1477594497257,1477594660207,1477594660207,1477594847678,1477594925871,1477594925871,1477595092841,1477595153256,1477595153256,1477595308236,1477595342730,1477595342730,1477595348375,1477595348375,1477595499986,1477595503682,1477595503682,1477595794118,1477595795512,1477595795512]
y2Demand = [10,10,12,12,12,14,14,14,17,17,17,17,17,5,5,5,3,3,3,3]
xTicksBarrier = [1477594847681,1477594847707,1477595092842,1477595095539,1477595308236,1477595310484,1477595342730,1477595344870,1477595499987,1477595501965,1477595794118,1477595794816]
xTicksBarrierLabels = ["b1","b1","b2","b2","b3","b3","b4","b4","b5","b5","b6","b6"]
xTicksSuperStep = [1477594583112,1477594659407,1477594847709,1477594925244,1477595095539,1477595152449,1477595310484,1477595339032,1477595344870,1477595348179,1477595501965,1477595503470,1477595794816,1477595795203]
xTicksSuperStepLabels = ["s0","s0","s1","s1","s2","s2","s3","s3","s4","s4","s5","s5","s6","s6"]

yAverageProcSpeed = [70960.0,68752.58333333333,48827.92857142857,24017.41176470588,2843.3529411764707,1366.8,335.6666666666667]
yElasticOverhead = [166383,146220,133666,0,141000,275513,0]

minX = min(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
maxX = max(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
normMaxX = (maxX - minX) / 1000

def subtract(x):
    return (x - minX) / 1000

x1Supply = list(map(subtract, x1Supply))
x2Demand = list(map(subtract, x2Demand))
xTicksBarrier = list(map(subtract, xTicksBarrier))
xTicksSuperStep = list(map(subtract, xTicksSuperStep))
yAverageProcSpeed = list(map(lambda x: x / 1000, yAverageProcSpeed))
yElasticOverhead = list(map(lambda x: x / 1000, yElasticOverhead))

xTickBarrierStart = [xTicksBarrier[i] for i in range(len(xTicksBarrier)) if i % 2 == 0]
xTickBarrierEnd = [xTicksBarrier[i] for i in range(len(xTicksBarrier)) if i % 2 == 1]
xTicksBarrierLabelsStart = [xTicksBarrierLabels[i] for i in range(len(xTicksBarrierLabels)) if i % 2 == 0]
xTicksBarrierLabelsEnd = [xTicksBarrierLabels[i] for i in range(len(xTicksBarrierLabels)) if i % 2 == 1]
xTicksSuperStep = [xTicksSuperStep[i] for i in range(len(xTicksSuperStep)) if i % 2 == 0]
xTicksSuperStepLabels = [xTicksSuperStepLabels[i] for i in range(len(xTicksSuperStepLabels)) if i % 2 == 0]

barWidths = [xTicksSuperStep[i] - xTicksSuperStep[i - 1] for i in range(1, len(xTicksSuperStep))]
barWidths.append(normMaxX)

fig = plt.figure()
supAxes = fig.add_axes((0.1, 0.5, 0.8, 0.0))
ax1 = fig.add_axes((0.1, 0.6, 0.8, 0.4))
ax2 = ax1.twinx()
barChart = fig.add_axes((0.1, 0.1, 0.8, 0.3))
barChart.set_xlim([0.0, normMaxX])

barChart.bar(xTicksSuperStep, yAverageProcSpeed, barWidths, color='r')
barChart.bar(xTicksSuperStep, yElasticOverhead, barWidths, bottom=yAverageProcSpeed)
barChart.set_xlabel('time (s)')
barChart.set_ylabel('time (s)')

ax1.set_ylim([0.0, 21])
ax2.set_ylim([0.0, 21])

ax1.set_xlim([0.0, normMaxX])
supAxes.set_xlim([0.0, normMaxX])

ax1.plot(x1Supply, y1Supply)
ax1.set_xlabel('time (s)')
# Make the y-axis label and tick labels match the line color.
ax1.set_ylabel('supply (machines)', color='b')
for tl in ax1.get_yticklabels():
    tl.set_color('b')

ax2.set_ylabel('demand (machines)', color='r')
ax2.plot(x2Demand, y2Demand, 'r')

for tl in ax2.get_yticklabels():
    tl.set_color('r')

supAxes.yaxis.set_visible(False)
supAxes.set_xlabel('superstep')
supAxes.set_xticks(xTicksSuperStep)
supAxes.set_xticklabels(xTicksSuperStepLabels)

plt.show()
      