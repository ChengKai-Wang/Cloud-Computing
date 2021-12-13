[TOC]
# Cloud Computing Practice 1 MapReduce
![](https://i.imgur.com/Wc0QtjL.png)
* 我們猜測：Hadoop在呼叫Combiner前，會將Map完的結果相同key的value做合併，因此在Combiner的value為一個iterable的list。
* 舉例：('SQL', 1), ('DW', 1), ('SQL', 1)進Combiner前會變成
('SQL', [1, 1])、('DW', [1])再各自call combiner
## Practice 1 : Email Count
### Input File Format
```
abcde@gmail.com
fghijk@yahoo.com.tw
lmnop@msn.com
```
### Output File Format
```
gmail.com 1
yahoo.com.tw 1
msn.com 1
```
### 執行結果
![](https://i.imgur.com/mI6kwmj.png)

### 思路
* Input的格式為xxx@mail，先將字串以@做分割。
* 可以發現需要的mail必定出現在偶數個位置
### Code
```java=
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class practice1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "@");
      int count = 0;
      while (itr.hasMoreTokens()) {
        count += 1;
        if ((count % 2) == 0) {
          word.set(itr.nextToken());
          context.write(word, one);
        }
        else{
          itr.nextToken();
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(practice1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
```

### 編譯&放到Hadoop上跑指令
```bash=
# compile java code
$ hadoop com.sun.tools.javac.Main practice1.java
$ jar cf wc.jar practice1*.class

# 刪除hadoop上的output資料夾
$ hadoop fs -rm -r /user/`whoami`/practice1/output

# 執行
$ hadoop jar wc.jar practice1 /user/`whoami`/practice1/input /user/`whoami`/practice1/output

# 看執行結果
$ hadoop fs -cat /user/`whoami`/practice1/output/part-r-00000

```

## Practice 2 : Inverted Index
![](https://i.imgur.com/3I64Lxz.png)
### Input File Format
```bash=
#File1
$ echo "MapReduce is simple" > file1.txt

#File2
$ echo "MapReduce is powerful is simple" > file2.txt

#File3
$ echo "Hello MapReduce bye MapReduce" > file3.txt
```

### Output File Format
```
Hello     file3.txt:1;
MapReduce file3.txt:2;file2.txt:1;file1.txt:1;
bye       file3.txt:1;
is        file2.txt:2;file1.txt:1;
powerful  file2.txt:1;
simple    file2.txt:1;file1.txt:1;
```






### Code
```java=
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class practice2 {

    public static class TokenizerMapper
         extends Mapper<Object, Text, Text, Text>{
      // Output types of a combiner must match output types of a mapper
      //private final static IntWritable one = new IntWritable(1);
      Text one = new Text("1");
      private Text word = new Text();
  
      public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            // Get filename
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            // Concat word and filename as a key
            word.set(String.format("%s:%s", itr.nextToken(), filename));
            // e.g. MapReduce:file1.txt 1
            context.write(word, one);
        }
      }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            // In order to reset the key and values, we need to override the [combine]
            // Original (key, value) pair: MapReduce:file1.txt 1
            // For Reducer use, (key, value) pair has to be changed to: MapReduce file1.txt:1 (output format)
            int sum = 0;
            for (Text val : values){
                sum += Integer.valueOf(val.toString());
            }
            String keys[] = key.toString().split(":");
            key.set(keys[0]);
            result.set(String.format("%s:%d", keys[1], sum));
            context.write(key, result);

        }
    }
  
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
      private Text result = new Text();
  
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileList = "";
        // concat all values with the same key
        for (Text val : values) {
          fileList += val.toString();
          fileList += ";";
        }
        result.set(fileList);
        context.write(key, result);
      }
    }
  
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Inverted Index");
      job.setJarByClass(practice2.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(Combine.class);
      job.setReducerClass(Reduce.class);
      job.setMapOutputKeyClass(Text.class);  
      job.setMapOutputValueClass(Text.class); 
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
```
### 執行結果
![](https://i.imgur.com/hKQO4aV.png)

### 注意
1. Mapper與Reducer的output type必須一致。 例如：Mapper的(key, value)是(Text, IntWritable)，Combiner也必須相同。
2. Mapper, Combiner, Reducer的code都要宣告為static class。

## Reference
1. http://puremonkey2010.blogspot.com/2013/11/mapreduce-inverted-index.html
2. https://hadoop.apache.org/docs/stable/api/index.html?org/apache/hadoop/mapreduce/Mapper.html
3. https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapreduce/Reducer.html
4. https://stackoverflow.com/questions/16196931/runtimeexception-java-lang-nosuchmethodexception-tfidfreduce-init
5. https://stackoverflow.com/questions/30546957/wrong-value-class-class-org-apache-hadoop-io-text-is-not-class-org-apache-hadoo
6. https://piazza-resources.s3.amazonaws.com/ist3pwd6k8p5t/iu5gqbsh8re6mj/OReilly.Hadoop.The.Definitive.Guide.4th.Edition.2015.pdf
7. https://blog.alantsai.net/posts/2017/12/data-science-series-09-hadoop-map-reduce-java-wordcount-example?fbclid=IwAR0b4lj0Qi5xlc1eAAtLJZB9YwcYl1Ncn3t_swR8TgVUcqVxXYl-rD-Jxs4
