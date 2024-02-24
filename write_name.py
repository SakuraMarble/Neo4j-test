# 打开文件，'a' 表示追加模式
with open('result/results_time.csv', 'w') as f:
    # 写入一行内容并添加换行符
    f.write('PageRank,Dijkstra,WCC,LPA\n')