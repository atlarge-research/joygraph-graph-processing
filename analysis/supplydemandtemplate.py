import matplotlib.pyplot as plt

x1Supply = [1475608680429,1475608766628,1475608766628,1475608857214,1475608857214,1475608895674,1475608895674,1475608980139,1475608980139,1475609013847,1475609013847,1475609086517,1475609086517,1475609107759,1475609107759,1475609110700,1475609110700,1475609184419,1475609184419,1475609186506,1475609186506,1475609331615,1475609331615,1475609332332,1475609332332]
y1Supply = [10,10,10,10,12,12,12,12,14,14,14,14,17,17,17,17,17,17,5,5,5,5,3,3,3]
x2Demand = [1475608680429,1475608766628,1475608766628,1475608857214,1475608895674,1475608895674,1475608980139,1475609013847,1475609013847,1475609086517,1475609107759,1475609107759,1475609110700,1475609110700,1475609184419,1475609186506,1475609186506,1475609331615,1475609332332,1475609332332]
y2Demand = [10,10,12,12,12,14,14,14,17,17,17,17,17,5,5,5,3,3,3,3]
xTicksBarrier = [1475608857217,1475608857240,1475608980139,1475608981646,1475609086517,1475609087792,1475609107759,1475609108886,1475609184419,1475609185443,1475609331615,1475609332002]
xTicksBarrierLabels = ["b0","b0","b1","b1","b2","b2","b3","b3","b4","b4","b5","b5"]
xTicksSuperStep = [1475608725989,1475608765682,1475608857242,1475608894940,1475608981647,1475609009426,1475609087792,1475609105408,1475609108886,1475609110628,1475609185443,1475609186399,1475609332002,1475609332189]
xTicksSuperStepLabels = ["s0","s0","s1","s1","s2","s2","s3","s3","s4","s4","s5","s5","s6","s6"]

minX = min(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
maxX = max(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)


def subtract(x):
    return (x - minX) / 1000

x1Supply = list(map(subtract, x1Supply))
x2Demand = list(map(subtract, x2Demand))
xTicksBarrier = list(map(subtract, xTicksBarrier))
xTicksSuperStep = list(map(subtract, xTicksSuperStep))

fig = plt.figure()
barrAxes = fig.add_axes((0.1, 0.1, 0.8, 0.0))
supAxes = fig.add_axes((0.1, 0.2, 0.8, 0.0))
ax1 = fig.add_axes((0.1, 0.3, 0.8, 0.6))

ax1.plot(x1Supply, y1Supply)
ax1.set_xlabel('time (s)')
# Make the y-axis label and tick labels match the line color.
ax1.set_ylabel('supply')
for tl in ax1.get_yticklabels():
    tl.set_color('b')

ax2 = ax1.twinx()
ax2.set_ylabel('demand')
ax2.plot(x2Demand, y2Demand, 'r')

for tl in ax2.get_yticklabels():
    tl.set_color('r')

supAxes.yaxis.set_visible(False)
supAxes.set_xlabel('superstep')
supAxes.set_xticks(xTicksSuperStep)
supAxes.set_xticklabels(xTicksSuperStepLabels)

barrAxes.yaxis.set_visible(False)
barrAxes.set_xticks(xTicksBarrier)
barrAxes.set_xticklabels(xTicksBarrierLabels)
barrAxes.set_xlabel("barrier")

plt.show()