import java.util.*;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class KeywordCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text filenameCount = new Text();
        private Text keyword = new Text();

        JobConf conf;

        public void configure(JobConf job) {
            this.conf = job;
        }

        public void map(LongWritable docID, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            // get the current file name
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String filename = "" + fileSplit.getPath().getName();

            int argc = Integer.parseInt(conf.get("argc"));
            java.util.Map<String, Integer> keywords = new HashMap<String, Integer>();

            for (int i = 0; i < argc; i++) {
                String inputString = conf.get("keyword" + i);

                keywords.put(inputString, 0);


            }

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {

                String token = tokenizer.nextToken();
                for (java.util.Map.Entry<String, Integer> me : keywords.entrySet()) {
                    if (token.equalsIgnoreCase(me.getKey())) {

                        keywords.put(me.getKey(), me.getValue() + 1);

                    }
                }
            }

            for (java.util.Map.Entry<String, Integer> me : keywords.entrySet()) {

                keyword.set(me.getKey());
                filenameCount.set(filename + "_" + me.getValue());

                output.collect(keyword, filenameCount);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            java.util.Map<String, Integer> filenameCounts = new HashMap<String, Integer>();
            Text docListText = new Text();

            while (values.hasNext()) {
                String filenameCount = values.next().toString();
                String[] filenameCountSplit = filenameCount.split("_");

                if (!filenameCounts.containsKey(filenameCountSplit[0])) {
                    filenameCounts.put(filenameCountSplit[0], Integer.parseInt(filenameCountSplit[1]));
                } else {
                    filenameCounts.put(filenameCountSplit[0], filenameCounts.get(filenameCountSplit[0]) + Integer.parseInt(filenameCountSplit[1]));
                }
            }
            List<String> docList = new ArrayList<>();

            for (java.util.Map.Entry<String, Integer> me : filenameCounts.entrySet()) {
                docList.add(me.getKey() + "_" + me.getValue());
            }

            Collections.sort(docList, (String str1, String str2) -> {

                String[] str1Strings = str1.split("_");
                String[] str2Strings = str2.split("_");

                return Integer.parseInt(str2Strings[1]) - Integer.parseInt(str1Strings[1]);
            });


            docListText.set(String.join(":", docList));
            output.collect(key, docListText);
        }
    }


    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(KeywordCount.class);

        conf.setJobName("keywordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        //conf.setNumReduceTasks(0);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.set("argc", String.valueOf(args.length - 2));
        for (int i = 0; i < args.length - 2; i++) {
            conf.set("keyword" + i, args[i + 2]);
        }

        JobClient.runJob(conf);
    }
}
