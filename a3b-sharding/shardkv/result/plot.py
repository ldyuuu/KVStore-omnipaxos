import numpy as np
import matplotlib.pyplot as plt

# 读取延迟文件
latency_a = np.loadtxt("latenciesA.txt")
latency_b = np.loadtxt("latenciesB.txt")
latency_c = np.loadtxt("latenciesC.txt")
print(f"Max Latency A: {np.max(latency_a)} ms")
print(f"Max Latency B: {np.max(latency_b)} ms")
print(f"Max Latency C: {np.max(latency_c)} ms")
print(f"Min Latency A: {np.min(latency_a)} ms")
print(f"Min Latency B: {np.min(latency_b)} ms")
print(f"Min Latency C: {np.min(latency_c)} ms")
# 计算 CDF
def compute_cdf(data):
    sorted_data = np.sort(data)
    cdf = np.arange(1, len(data) + 1) / len(data)
    return sorted_data, cdf

lat_a, cdf_a = compute_cdf(latency_a)
lat_b, cdf_b = compute_cdf(latency_b)
lat_c, cdf_c = compute_cdf(latency_c)

# 绘制 CDF 图
plt.figure(figsize=(10, 6))
plt.plot(lat_a, cdf_a, label="Benchmark A", linewidth=2, color="blue")
plt.plot(lat_b, cdf_b, label="Benchmark B", linewidth=2, color="yellow")
plt.plot(lat_c, cdf_c, label="Benchmark C", linewidth=2, color="red")
plt.xscale("log")
plt.xlabel("Latency (ns)")
plt.ylabel("CDF")
plt.title("Latency CDFs - Benchmark A, B, and C")
plt.legend()
plt.grid(True, which="both", linestyle="--", linewidth=0.5)
plt.savefig("LatencyBenchmark(CloudLab-r320).png")
plt.show()

import numpy as np
import matplotlib.pyplot as plt

# 读取数据
data_a = np.loadtxt("throughputA.txt")  # 示例文件 throughputA.txt
data_b = np.loadtxt("throughputB.txt")  # 其他类似文件
data_c = np.loadtxt("throughputC.txt")

# 提取吞吐量（第二列）
throughput_a = data_a[:, 1]
throughput_b = data_b[:, 1]
throughput_c = data_c[:, 1]

# 计算每个基准的平均吞吐量
mean_throughput_a = np.mean(throughput_a)
mean_throughput_b = np.mean(throughput_b)
mean_throughput_c = np.mean(throughput_c)

# 数据汇总
benchmarks = ["Benchmark A", "Benchmark B", "Benchmark C"]
mean_throughput_values = [mean_throughput_a, mean_throughput_b, mean_throughput_c]

# 绘制柱状图
plt.figure(figsize=(8, 6))
plt.bar(benchmarks, mean_throughput_values, color=["blue", "yellow", "red"])

# 设置 Y 轴范围
plt.ylim(0, 150)  # 固定 Y 轴范围为 0 到 150

# 添加标签和标题
plt.xlabel("Benchmarks")
plt.ylabel("Average Throughput (Records / Second)")
plt.title("Throughput Benchmark")
plt.grid(axis="y", linestyle="--", linewidth=0.5)
plt.savefig("ThroughputBenchmark(CloudLab-r320).png")
# 显示图表
plt.show()
