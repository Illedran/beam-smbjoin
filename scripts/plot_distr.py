from scipy.stats import zipf
from matplotlib import pyplot as plt
import numpy as np
from itertools import chain

b = 32
x = np.loadtxt("test.csv", dtype=int, converters={0: lambda x: int(x.strip('"'),16)}, encoding=None)

B = np.zeros(b, dtype=int)

for i in x:
    B[i % b] += 1

B[::-1].sort()
print(B)
plt.loglog(np.arange(1,b+1), B, marker='o',
    linestyle='solid', linewidth=0.5, markersize=4)

plt.show()



