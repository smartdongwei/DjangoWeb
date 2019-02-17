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
