import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class DatasetSpliter {
    String dataSetPath;
    String trainSetPath;
    String testSetPath;
    ArrayList<String> classNames;

    float trainSetRatio = 0.8f;

    private boolean[] getRandomSplit(int dataSetSize) {
        int trainSetSize = (int) (trainSetRatio * dataSetSize);
        boolean[] isTest = new boolean[dataSetSize];
        for (int i = 0; i < dataSetSize - trainSetSize; i++) {
            int index = (int) (Math.random() * dataSetSize);
            if (isTest[index]) {
                i--;
            }else {
                isTest[index] = true;
            }
        }

        return isTest;
    }

    private ArrayList<File> getDataset() {
        File dir = new File(dataSetPath);
        File[] classes = dir.listFiles();
        ArrayList<File> files = new ArrayList<File>();
        assert classes != null;
        for (File c: classes
        ) {
            File[] fs = c.listFiles();
            for (int i = 0; i < (Objects.requireNonNull(fs)).length; i++) {
                classNames.add(c.getName());
            }
            files.addAll(Arrays.asList(fs));
        }

        return files;
    }

    private void createTrainAndTestSet(ArrayList<File> files) {
        int datasetSize = files.size();
        boolean[] isTest = getRandomSplit(datasetSize);
        String destinationPath;
        File f;
        for (int i = 0; i < datasetSize; i++) {
            f = files.get(i);
            destinationPath = isTest[i]? testSetPath: trainSetPath;
            DatasetSpliter.copyFile(
                    f.getPath(),
                    destinationPath + '\\' + classNames.get(i) + '_' + f.getName());
        }
    }

    public DatasetSpliter(String dp, String trp, String tp){
        this.dataSetPath = dp;
        this.trainSetPath = trp;
        this.testSetPath = tp;

        this.classNames = new ArrayList<>();
    }

    public void setTrainSetRatio(float ratio){
        trainSetRatio = ratio;
    }

    public void split(){
        ArrayList<File> files = getDataset();
        createTrainAndTestSet(files);
    }

    public static void copyFile(String sourcePath, String destinationPath) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sourcePath)));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destinationPath)));
            String t;
            while (true) {
                if ((t = br.readLine()) != null) {
                    bw.write(t + "\n");
                } else {
                    break;
                }
            }

            br.close();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DatasetSpliter spliter = new DatasetSpliter(
                "X:\\Document\\g1\\hadoop\\NBCorpus\\selected_country",
                "X:\\Document\\g1\\hadoop\\result\\selected_country\\train",
                "X:\\Document\\g1\\hadoop\\result\\selected_country\\test"
        );
        // 按8:2随机划分训练集和测试集
        spliter.setTrainSetRatio(0.8f);
        spliter.split();
    }
}
