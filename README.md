# Bayes分类器的mapreduce实现
## 数据集
dataset下的NBCorpus下有两个目录，每个目录是一个数据集。每个目录下的每个文件夹都是一个类，文件夹下是该类的文档。由于各个类的文档数差距很大，需选取几个文档数相近的类，如{AUSTR,CHINA,FRA}
## 运行
|  步骤   | 类  | 作用  | 参数  |
|  ----  | ----  | ----  | ----  |
| 1  | DatasetSpliter | 划分训练集和测试集  | 代码内设置：数据集路径、训练集路径、测试集路径、划分比例  |
| 2  | DocumentCount | 计算先验概率  | 训练集路径、结果存放路径  |
| 3  | TermCount | 计算条件概率  | 训练集路径、结果存放路径  |
| 4  | Prediction | 预测测试集  | 测试集路径、结果存放路径、2的结果路径、3的结果路径  |
| 5  | Evaluation | 计算评价指标  | 4的结果路径、所有类名(以“，”分割)  |