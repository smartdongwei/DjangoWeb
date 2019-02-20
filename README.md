# 学习资料
http://www.54manong.com/ebook/%E5%A4%A7%E6%95%B0%E6%8D%AE/20181208232851/Hadoop%E5%AE%9E%E6%88%98%E7%AC%AC2%E7%89%88-%E9%99%86%E5%98%89%E6%81%92/Hadoop%E5%AE%9E%E6%88%98%E7%AC%AC2%E7%89%88-%E9%99%86%E5%98%89%E6%81%92.html#calibre_link-248

# mapReduce的练习

KmeansMR.java 是kmeans在hadoop中的运用

WordCount.java 是练习

## 运行mapreduce jar包
输出路径需要先删除
bin/hadoop fs -rmr /home/wang/test/output

输入路径要在hadoop中创建
bin/hadoop fs -mkdir -p /home/wang/test/output

运行方法
bin/hadoop jar //home/wang/IdeaProjects/wordCount/out/artifacts/wordCount_jar/wordCount.jar  /home/wang/test/input /home/wang/test/output
