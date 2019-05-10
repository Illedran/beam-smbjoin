from matplotlib import pyplot as plt

B = 64
r = range(1, B+1)
n = 120e6

for s in [0, 0.25, 0.6, 1.1]:
    H = [1./(i**s) for i in r]
    H_sum = sum(H)
    data = [n * H[i-1]/H_sum for i in r]
    plt.loglog(r, data, label=f'{s:.2f}', marker='o', markersize=4, linewidth=0.5)

plt.xlabel('Bucket rank')
plt.ylabel("Bucket size")
plt.legend()
plt.show()
