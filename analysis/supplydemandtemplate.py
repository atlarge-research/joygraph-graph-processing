import matplotlib.pyplot as plt

x1Supply = [1475716887225,1475717054644,1475717054644,1475717232693,1475717232693,1475717310389,1475717310389,1475717471402,1475717471402,1475717533058,1475717533058,1475717675257,1475717675257,1475717817775,1475717817775,1475717988680,1475717988680,1475718038323,1475718038323,1475718160941,1475718160941,1475718211232,1475718211232,1475718258213,1475718258213,1475718304438,1475718304438,1475718351002,1475718351002,1475718397605,1475718397605]
y1Supply = [10,10,10,10,12,12,12,12,14,14,14,14,16,16,16,16,18,18,18,18,20,20,20,20,20,20,20,20,20,20,20]
x2Demand = [1475716887225,1475717054644,1475717054644,1475717232693,1475717310389,1475717310389,1475717471402,1475717533058,1475717533058,1475717675257,1475717817775,1475717817775,1475717988680,1475718038323,1475718038323,1475718160941,1475718211232,1475718211232,1475718258213,1475718258213,1475718304438,1475718304438,1475718351002,1475718351002,1475718397605,1475718397605]
y2Demand = [10,10,12,12,12,14,14,14,16,16,16,18,18,18,20,20,20,20,20,20,20,20,20,20,20,20]
xTicksBarrier = [1475717232696,1475717232728,1475717471402,1475717474550,1475717675258,1475717677476,1475717988681,1475717991261,1475718160943,1475718162504,1475718211232,1475718212828,1475718258213,1475718259824,1475718304438,1475718306197,1475718351002,1475718352559]
xTicksBarrierLabels = ["b0","b0","b1","b1","b2","b2","b3","b3","b4","b4","b5","b5","b6","b6","b7","b7","b8","b8"]
xTicksSuperStep = [1475716973929,1475717053965,1475717232730,1475717309625,1475717474551,1475717532694,1475717677476,1475717817091,1475717991262,1475718037585,1475718162505,1475718210798,1475718212829,1475718257297,1475718259824,1475718304120,1475718306197,1475718349552,1475718352559,1475718396952]
xTicksSuperStepLabels = ["s0","s0","s1","s1","s2","s2","s3","s3","s4","s4","s5","s5","s6","s6","s7","s7","s8","s8","s9","s9"]

minX = min(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
maxX = max(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
normMaxX = (maxX - minX) / 1000

def subtract(x):
    return (x - minX) / 1000

x1Supply = list(map(subtract, x1Supply))
x2Demand = list(map(subtract, x2Demand))
xTicksBarrier = list(map(subtract, xTicksBarrier))
xTicksSuperStep = list(map(subtract, xTicksSuperStep))

xTickBarrierStart = [xTicksBarrier[i] for i in range(len(xTicksBarrier)) if i % 2 == 0]
xTickBarrierEnd = [xTicksBarrier[i] for i in range(len(xTicksBarrier)) if i % 2 == 1]
xTicksBarrierLabelsStart = [xTicksBarrierLabels[i] for i in range(len(xTicksBarrierLabels)) if i % 2 == 0]
xTicksBarrierLabelsEnd = [xTicksBarrierLabels[i] for i in range(len(xTicksBarrierLabels)) if i % 2 == 1]
xTicksSuperStep = [xTicksSuperStep[i] for i in range(len(xTicksSuperStep)) if i % 2 == 0]
xTicksSuperStepLabels = [xTicksSuperStepLabels[i] for i in range(len(xTicksSuperStepLabels)) if i % 2 == 0]

fig = plt.figure()
barrAxes = fig.add_axes((0.1, 0.15, 0.8, 0.0))
supAxes = fig.add_axes((0.1, 0.3, 0.8, 0.0))
ax1 = fig.add_axes((0.1, 0.4, 0.8, 0.6))
ax2 = ax1.twinx()

ax1.set_ylim([0.0, 21])
ax2.set_ylim([0.0, 21])

ax1.set_xlim([0.0, normMaxX])
supAxes.set_xlim([0.0, normMaxX])
barrAxes.set_xlim([0.0, normMaxX])
barrAxes2 = barrAxes.twiny()
barrAxes2.set_xlim([0.0, normMaxX])

ax1.plot(x1Supply, y1Supply)
ax1.set_xlabel('time (s)')
# Make the y-axis label and tick labels match the line color.
ax1.set_ylabel('supply', color='b')
for tl in ax1.get_yticklabels():
    tl.set_color('b')

ax2.set_ylabel('demand', color='r')
ax2.plot(x2Demand, y2Demand, 'r')

for tl in ax2.get_yticklabels():
    tl.set_color('r')

supAxes.yaxis.set_visible(False)
supAxes.set_xlabel('superstep')
supAxes.set_xticks(xTicksSuperStep)
supAxes.set_xticklabels(xTicksSuperStepLabels)

barrAxes.yaxis.set_visible(False)
barrAxes.set_xticks(xTickBarrierStart)
barrAxes.set_xticklabels(xTicksBarrierLabelsStart)
barrAxes.set_xlabel("barrier")

barrAxes2.set_xticks(xTickBarrierEnd)
barrAxes2.set_xticklabels(xTicksBarrierLabelsEnd)

plt.show()