import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**在mapreduce程序中实现lmeans算法
 * 1. main函数读取质心文件
 * 2. 将质心的字符串放到configuration中
 * 3. 在mapper类重写setup方法，获取到configuration的质心内容，解析成二维数组的形式，代表质心
 * 4. mapper类中的map方法读取样本文件，跟所有的质心比较，得出每个样本跟哪个质心最近，然后输出<质心，样本>
 * 5. reducer类中重新计算质心，如果重新计算出来的质心跟进来时的质心一致，那么自定义的counter加1
 * 6. main中获取counter的值，看是否等于质心，如果不相等，那么继续迭代，否在退出
 *
 */
public class KmeansMR {

    private static String FLAG = "KCLUSTER";

    public static class TokenizerMapper extends Mapper<Object, Text,Text  , IntWritable> {
        double[][] centers = new double[2][];
        String[] centerstrArray = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //将放在context中的聚类中心转换为数组的形式，方便使用
            String kmeansS = context.getConfiguration().get(FLAG);
            centerstrArray = kmeansS.split("\t");
            for(int i = 0; i < centerstrArray.length; i++) {
                String[] segs = centerstrArray[i].split(",");
                centers[i] = new double[segs.length];
                for(int j = 0; j < segs.length; j++) {
                    centers[i][j] = Double.parseDouble(segs[j]);
                }
            }
        }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] segs = line.split(",");
        double[] sample = new double[segs.length];
        for(int i = 0; i < segs.length; i++) {
            sample[i] = Float.parseFloat(segs[i]);
        }
        //求得距离最近的质心
        double min = Double.MAX_VALUE;
        int index = 0;
        for(int i = 0; i < centers.length; i++) {
            double dis = distance(centers[i], sample);
            if(dis < min) {
                min = dis;
                index = i;
            }
        }

        context.write(new Text(centerstrArray[index]), new Text(line));
        }
     }
    public static class IntSumReducer
            extends Reducer<Text,Text, NullWritable,Text> {

        Counter counter = null;

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double[] sum = new double[2];
            int size = 0;
            //计算对应维度上值的加和，存放在sum数组中
            for(Text text : values) {
                String[] segs = text.toString().split(",");
                for(int i = 0; i < segs.length; i++) {
                    sum[i] += Double.parseDouble(segs[i]);
                }
                size ++;
            }

            //求sum数组中每个维度的平均值，也就是新的质心
            StringBuffer sb = new StringBuffer();
            for(int i = 0; i < sum.length; i++) {
                sum[i] /= size;
                sb.append(sum[i]);
                sb.append(",");
            }

            /**判断新的质心跟老的质心是否是一样的*/
            boolean flag = true;
            String[] centerStrArray = key.toString().split(",");
            for(int i = 0; i < centerStrArray.length; i++) {
                if(Math.abs(Double.parseDouble(centerStrArray[i]) - sum[i]) > 0.00000000001) {
                    flag = false;
                    break;
                }
            }
            //如果新的质心跟老的质心是一样的，那么相应的计数器加1
            if(flag) {
                counter = context.getCounter("myCounter", "kmenasCounter");
                counter.increment(1l);
            }
            context.write(null, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Path kMeansPath = new Path("/dsap/middata/kmeans/kMeans");	//初始的质心文件
        Path samplePath = new Path("/dsap/middata/kmeans/sample");	//样本文件
        //加载聚类中心文件
        Center center = new Center();
        String centerString = center.loadInitCenter(kMeansPath);

        int index = 0;	//迭代的次数
        while(index < 5) {

            Configuration conf = new Configuration();
            conf.set(FLAG, centerString);	//将聚类中心的字符串放到configuration中

            kMeansPath = new Path("/dsap/middata/kmeans/kMeans" + index);	//本次迭代的输出路径，也是下一次质心的读取路径

            /**判断输出路径是否存在，如果存在，则删除*/
            FileSystem hdfs = FileSystem.get(conf);
            if(hdfs.exists(kMeansPath)) hdfs.delete(kMeansPath);

            Job job = new Job(conf, "kmeans" + index);
            job.setJarByClass(KmeansMR.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, samplePath);
            FileOutputFormat.setOutputPath(job, kMeansPath);
            job.waitForCompletion(true);

            /**获取自定义counter的大小，如果等于质心的大小，说明质心已经不会发生变化了，则程序停止迭代*/
            long counter = job.getCounters().getGroup("myCounter").findCounter("kmenasCounter").getValue();
            if(counter == 2)	System.exit(0);
            /**重新加载质心*/
            center = new Center();
            centerString = center.loadCenter(kMeansPath);
            index ++;
        }
        System.exit(0);
    }


    /**
     * 计算两点之间的距离  欧式距离
     * @param a
     * @param b
     * @return
     */
    public static double distance(double[] a, double[] b) {

        if (a == null || b == null || a.length != b.length) return Double.MAX_VALUE;
        double dis = 0;
        for (int i = 0; i < a.length; i++) {
            dis += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(dis);

    }
}
