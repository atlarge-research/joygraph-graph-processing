import matplotlib.pyplot as plt

x1Supply = [1477594497257,1477594660207,1477594660207,1477594847678,1477594847678,1477594925871,1477594925871,1477595092841,1477595092841,1477595153256,1477595153256,1477595308236,1477595308236,1477595342730,1477595342730,1477595348375,1477595348375,1477595499986,1477595499986,1477595503682,1477595503682,1477595794118,1477595794118,1477595795512,1477595795512]
y1Supply = [10,10,10,10,12,12,12,12,14,14,14,14,17,17,17,17,17,17,5,5,5,5,3,3,3]
x2Demand = [1477594497257,1477594660207,1477594660207,1477594847678,1477594925871,1477594925871,1477595092841,1477595153256,1477595153256,1477595308236,1477595342730,1477595342730,1477595348375,1477595348375,1477595499986,1477595503682,1477595503682,1477595794118,1477595795512,1477595795512]
y2Demand = [10,10,12,12,12,14,14,14,17,17,17,17,17,5,5,5,3,3,3,3]
xTicksSuperStep = [1477594583112,1477594659407,1477594847709,1477594925244,1477595095539,1477595152449,1477595310484,1477595339032,1477595344870,1477595348179,1477595501965,1477595503470,1477595794816,1477595795203]
xTicksSuperStepLabels = ["s0","s0","s1","s1","s2","s2","s3","s3","s4","s4","s5","s5","s6","s6"]

minX = min(x1Supply + x2Demand + xTicksSuperStep)
maxX = max(x1Supply + x2Demand + xTicksSuperStep)
normMaxX = (maxX - minX) / 1000

def subtract(x):
    return (x - minX) / 1000

x1Supply = list(map(subtract, x1Supply))
x2Demand = list(map(subtract, x2Demand))
xTicksSuperStep = list(map(subtract, xTicksSuperStep))

xTicksSuperStep = [xTicksSuperStep[i] for i in range(len(xTicksSuperStep)) if i % 2 == 0]
xTicksSuperStepLabels = [xTicksSuperStepLabels[i] for i in range(len(xTicksSuperStepLabels)) if i % 2 == 0]

fig = plt.figure()
supAxes = fig.add_axes((0.1, 0.1, 0.8, 0.0))
ax1 = fig.add_axes((0.1, 0.2, 0.8, 0.8))
ax2 = ax1.twinx()

ax1.set_ylim([0.0, 21])
ax2.set_ylim([0.0, 21])

ax1.set_xlim([0.0, normMaxX])
supAxes.set_xlim([0.0, normMaxX])

p1 = ax1.plot(x1Supply, y1Supply)
p2 = ax2.plot(x2Demand, y2Demand, 'r')

ax1.set_xlabel('time (s)')
# Make the y-axis label and tick labels match the line color.
ax1.set_ylabel('machines')
ax1.legend((p1[0], p2[0]), ('supply', 'demand'), loc = 0)

supAxes.yaxis.set_visible(False)
supAxes.set_xlabel('superstep')
supAxes.set_xticks(xTicksSuperStep)
supAxes.set_xticklabels(xTicksSuperStepLabels)

plt.show()
      