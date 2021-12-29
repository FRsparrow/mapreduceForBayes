# Bayes分类器的mapreduce实现
## 数据集
dataset下的NBCorpus下有两个目录，每个目录是一个数据集。每个目录下的每个文件夹都是一个类，文件夹下是该类的文档。由于各个类的文档数差距很大，需选取几个文档数相近的类，如{AUSTR,CHINA,FRA}
## 运行
1. DatasetSpliter类按比例随机划分数据集为训练集和测试集
2. DocumentCount类计算各个类的先验概率
3. TermCount类计算各个类中各个单词出现的概率
4. Prediction类预测测试集
5. Evaluation类计算宏平均和微平均