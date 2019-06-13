from matplotlib import pyplot as plt
import numpy as np

B = int(50e6)
r = np.arange(B)+1
n = 6e9
cutoff=int(1e6)

for s in np.linspace(0,1.4,8):
    H = 1./(r**s)
    H_sum = H.sum()
    data = n * H/H_sum
    plt.loglog(r[:cutoff], data[:cutoff], label=f'{s:.2f}', linewidth=1)
    print(list(map(int,data[:8])))

plt.xlabel('Rank in key frequency table')
plt.ylabel("Key frequency")
plt.legend()
plt.show()
