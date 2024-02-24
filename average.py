import pandas as pd

# 读取 CSV 文件
df = pd.read_csv('result/results_time.csv')

# 计算每列的平均值
mean_values = df.mean()

# 将平均值转换为 DataFrame 并转置，使得每个算法成为一行
mean_df = pd.DataFrame(mean_values).transpose()

# 将结果写入新的 CSV 文件
mean_df.to_csv('result/result_final.csv', index=False)