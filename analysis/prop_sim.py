#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt

def simulate_propagation_times(num_components, num_samples):
    # Generate log-normal distributed propagation times for each component
    propagation_times = np.random.lognormal(mean=2, sigma=0.5, size=(num_samples, num_components))
    # Get the maximum propagation time for each sample (time for the last component to arrive)
    max_propagation_times = np.max(propagation_times, axis=1)
    return max_propagation_times

# Number of components (1 block + k blobs)
num_blobs_list = [0, 1, 2, 3, 4, 5, 6]
num_samples = 80000000

# Store max times, average times, and median times for each number of blobs
max_times_all_blobs = []
average_times = []
median_times = []

# Colors specified by the user
colors = ['#f94144', '#f3722c', '#f8961e', '#f9c74f', '#90be6d', '#43aa8b', '#577590']

# Plot histograms for different numbers of blobs as line plots
plt.figure(figsize=(14, 8))

for num_blobs, color in zip(num_blobs_list, colors):
    max_times = simulate_propagation_times(num_blobs + 1, num_samples)
    max_times_all_blobs.append(max_times)
    average_times.append(np.mean(max_times))
    median_times.append(np.median(max_times))
    hist, bins = np.histogram(max_times, bins=1000, density=True)
    center = (bins[:-1] + bins[1:]) / 2
    plt.plot(center, hist, label=f'{num_blobs} blobs', color=color)

plt.title('Histogram of Propagation Times for the Last Component to Arrive')
plt.xlabel('Time (arbitrary units)')
plt.ylabel('Frequency')
plt.legend()
plt.grid(True)
plt.xlim(0, 50)
plt.show()

# Plot box plot for different numbers of blobs
plt.figure(figsize=(14, 8))
plt.boxplot(max_times_all_blobs, labels=num_blobs_list)
plt.title('Box Plot of Propagation Times for the Last Component to Arrive')
plt.xlabel('Number of Blobs')
plt.ylabel('Time (arbitrary units)')
plt.grid(True)
plt.show()

# Plot average and median times for different numbers of blobs
plt.figure(figsize=(14, 8))
plt.plot(num_blobs_list, average_times, marker='o', label='Average Time')
plt.plot(num_blobs_list, median_times, marker='x', label='Median Time')
plt.title('Average and Median Propagation Times for the Last Component to Arrive')
plt.xlabel('Number of Blobs')
plt.ylabel('Time (arbitrary units)')
plt.legend()
plt.grid(True)
plt.show()