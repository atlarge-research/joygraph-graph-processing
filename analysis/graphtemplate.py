import matplotlib.pyplot as plt

# x = [0,1,2,3,4,5]
# y = [0,5,5,5,5,10]
#
# # err = [0.5,1,2,3,4,5]
#
# err = [[1,2],[3,4],[5,6],[3,4],[1,2],[5,5]]

x = [0, 1, 2]
y = [5, 4, 3]

err = [[4,1,6],[5,5,6]]

plt.errorbar(x,y,yerr = err)


plt.show()

