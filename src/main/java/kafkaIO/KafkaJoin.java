package kafkaIO;

/**
 * 利用kafka进行 Join 测试
 * Beam版本：2.3
 * @author: maqy
 * @date: 2018.09.22
 */

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class KafkaJoin {
    //用于输出Pcollection中的字符串
    static class printString extends DoFn<KV<String ,String> ,KV<String ,String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("c.element:" + c.element());
            c.output(c.element());
        }
    }
    //将每行数据分为 key value
    static class SetValue extends DoFn<String ,KV<String ,String>>{
        @ProcessElement
        public void processElement(ProcessContext c){
            System.out.println("c.element:"+c.element());
            String[] temps=c.element().split(",");
//            for(String temp:temps){
//                System.out.println(temp);
//            }
            KV<String ,String> kv=KV.of(temps[0],temps[1]);
            c.output(kv);
        }
    }

    //预处理，即将每行为 a,b De 的数据转化为KV<a,b>
    public static class Preprocess extends PTransform<PCollection<String>,PCollection<KV<String,String>>> {
        @Override
        public PCollection<KV<String,String>> expand(PCollection<String> lines){
            //String[] temps = lines.toString().split(",");
            PCollection<KV<String,String>> result = lines.apply(ParDo.of(new SetValue()));
            return result;
        }

    }

    //用于输出
    public static class FormatAsTextFn extends SimpleFunction<KV<String,KV<String,String>>, String> {
        @Override
        public String apply(KV<String,KV<String,String>> input) {
            return "key:"+input.getKey()+"   value:"+input.getValue();
        }
    }
    static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    public interface KafkaJoinOptions extends PipelineOptions,StreamingOptions {
        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("/home/maqy/Documents/beam_samples/output/test.txt")
        //Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();
        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("/home/maqy/文档/beam_samples/output/GroupbyTest")
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        KafkaJoinOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaJoinOptions.class);

        //options.setStreaming(true);
        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        //不知道为什么从kafka里读出来需要是<Long,String>格式
        PCollection<KV<Long,String>> source=p.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("test123")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                // the first 5 records.
                // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                // 要么读取一段时间，要么读取固定数量的记录
                //.withMaxNumRecords(10)
                .withMaxReadTime(Duration.millis(5000))
                .withoutMetadata() // PCollection<KV<Long, String>>
        );

        //将kafka中得到的<Long,String> 得到 String
        PCollection<String> kafkaLine=source.apply(Values.<String>create());
        //得到文件中的数据
        PCollection<String> fileLine=p.apply(TextIO.read().from("/home/maqy/桌面/output/kafkaJoin"));

        //预处理，即将每行为 a,b De 的数据转化为KV<a,b>
        PCollection<KV<String,String>> leftPcollection=kafkaLine.apply(new Preprocess());

        PCollection<KV<String,String>> rightPcollection=fileLine.apply(new Preprocess());

        PCollection<KV<String,KV<String,String>>> joinedPcollection = Join.innerJoin(leftPcollection,rightPcollection);
//
//        //results.apply(ParDo.of(new printString()));
        joinedPcollection.apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to("/home/maqy/桌面/output/kafkaJoinOutxxxxxxxx"));

        p.run().waitUntilFinish();
    }
}

