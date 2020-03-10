import numpy as np
import matplotlib.pyplot as plt
from hurst import compute_Hc, random_walk

# Use random_walk() function or generate a random walk series manually:
# series = random_walk(99999, cumprod=True)
np.random.seed(42)
# random_changes = 1. + np.random.randn(99999) / 1000.
# random_changes = 1. + np.random.randn(100) / 1000.
# series = np.cumprod(random_changes)  # create a random walk from random changes
random_changes = 1 + np.random.randn(100)
series = random_changes
print(series)

# Evaluate Hurst equation
"""
Kinds of series
The kind parameter of the compute_Hc function can have the following values:
'change': a series is just random values (i.e. np.random.randn(...))
'random_walk': a series is a cumulative sum of changes (i.e. np.cumsum(np.random.randn(...)))
'price': a series is a cumulative product of changes (i.e. np.cumprod(1+epsilon*np.random.randn(...))
"""
# H, c, data = compute_Hc(series, kind='price', simplified=True)
H, c, data = compute_Hc(series, kind='change', simplified=True)

# Plot
f, ax = plt.subplots()
# ax.plot(data[0], c*data[0]**H, color="deepskyblue")
ax.plot(data[0], c*np.array(data[0]), color="deepskyblue")
ax.scatter(data[0], data[1], color="purple")
ax.set_xscale('log')
ax.set_yscale('log')
ax.set_xlabel('Time interval')
ax.set_ylabel('R/S ratio')
ax.grid(True)
print("H={:.4f}, c={:.4f}".format(H,c))
plt.show()

