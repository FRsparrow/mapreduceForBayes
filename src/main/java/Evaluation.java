import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Evaluation {

    public static int[][] calConfusionMatrices(Path path, String[] classNames) throws IOException {
        int[][] confusionMatrices = new int[classNames.length+1][4];
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
        String line, className, predictedClassName;
        String[] cn;
        int index, lastIndex = classNames.length;

        while ((line = br.readLine()) != null) {
            cn = line.split("\\s+");
            className = cn[0].substring(0, cn[0].indexOf('_'));
            predictedClassName = cn[1];

            // 计算每个类的混淆矩阵: 0TP,1FP,2FN,3TN
            for (int i = 0; i < lastIndex; i++) {
                // 下标由实际类别、预测类别和当前混淆矩阵所属类别决定
                index = (2 * (className.equals(predictedClassName) ? 1 : 0) + (className.equals(classNames[i]) ? 1 : 0) + 1) % 4;
                confusionMatrices[i][index] += 1;
            }
        }
        for (int i = 0; i < lastIndex; i++) {
            for (int j = 0; j < 4; j++) {
                confusionMatrices[lastIndex][j] += confusionMatrices[i][j];
            }
        }

        br.close();
        return confusionMatrices;
    }

    // 计算宏平均指标
    public static double[] calMacro(int[][] confusionMatrices, int length) {
        double[] macro = new double[3];
        double PSum = 0, RSum = 0, F1Sum = 0;
        double P, R, F1;
        for (int i = 0; i < length; i++) {
            P = (double) confusionMatrices[i][0] / (confusionMatrices[i][0] + confusionMatrices[i][1]);
            R = (double) confusionMatrices[i][0] / (confusionMatrices[i][0] + confusionMatrices[i][2]);
            F1 = 2*P*R / (P+R);
            PSum += P;
            RSum += R;
            F1Sum += F1;
        }

        macro[0] = PSum / length;
        macro[1] = RSum / length;
        macro[2] = F1Sum / length;

        return macro;
    }

    // 计算微平均指标
    public static double[] calMicro(int[] microConfusionMatrix) {
        double[] micro = new double[3];
        micro[0] = (double) microConfusionMatrix[0] / (microConfusionMatrix[0] + microConfusionMatrix[1]);
        micro[1] = (double) microConfusionMatrix[0] / (microConfusionMatrix[0] + microConfusionMatrix[2]);
        micro[2] = 2 * micro[0] * micro[1] / (micro[0] + micro[1]);
        return micro;
    }

    /*
    args[0]:Prediction结果文件路径
    args[1]:所有类名，以","分割
    * */
    public static void main(String[] args) throws IOException {
        Path path = new Path(args[0]);
        String[] classNames = args[1].split(",");
        int numClass = classNames.length;
        int[][] confusionMatrices = calConfusionMatrices(path, classNames);

        System.out.println("各类混淆矩阵分别为");
        for (String className: classNames) {
            System.out.print(className + "\t\t");
        }
        System.out.println();
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < numClass; i++) {
                System.out.print(confusionMatrices[i][2*j] + "\t" + confusionMatrices[i][2*j+1] + "\t\t");
            }
            System.out.println();
        }
        System.out.println("微平均混淆矩阵为");
        System.out.println(confusionMatrices[3][0] + "\t" + confusionMatrices[3][1] + "\t\t");
        System.out.println(confusionMatrices[3][2] + "\t" + confusionMatrices[3][3]);

        double[] macro = calMacro(confusionMatrices, confusionMatrices.length-1);
        double[] micro = calMicro(confusionMatrices[confusionMatrices.length-1]);
        System.out.println("宏平均下Precision、Recall、F1分别为" + Arrays.toString(macro));
        System.out.println("微平均下Precision、Recall、F1分别为" + Arrays.toString(micro));
    }
}
