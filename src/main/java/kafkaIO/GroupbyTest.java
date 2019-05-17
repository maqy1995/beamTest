package kafkaIO;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GroupbyTest {
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
    public static class Preprocess extends PTransform<PCollection<String>,PCollection<KV<String,String>>>{
        @Override
        public PCollection<KV<String,String>> expand(PCollection<String> lines){
            //String[] temps = lines.toString().split(",");
            PCollection<KV<String,String>> result = lines.apply(ParDo.of(new SetValue()));
            return result;
        }

    }
    //用于输出
    public static class FormatAsTextFn extends SimpleFunction<KV<String, String>, String> {
        @Override
        public String apply(KV<String, String> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public interface GroupbyTestOptions extends PipelineOptions ,StreamingOptions{
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

    public static void main(String[] args){
        GroupbyTestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GroupbyTestOptions.class);
        options.setStreaming(true); //通过这里可以显示的指定批或流
        Pipeline p= Pipeline.create(options);

        PCollection<String> lines=p.apply(TextIO.read().from(options.getInputFile()));
        //预处理，即将每行为 a,b De 的数据转化为KV<a,b>
        PCollection<KV<String,String>> mmm=lines.apply(new Preprocess());
        //mmm.apply(ParDo.of(new printString()));
        //KV<String,String> reusult=mmm.apply(GroupByKey.create());
        PCollection<KV<String,Iterable<String>>> mmmResult=mmm.apply(GroupByKey.<String,String>create());
        PCollection<KV<String,String>> results= mmmResult.apply(ParDo.of(new DoFn<KV<String,Iterable<String>>, KV<String,String>>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                String key = c.element().getKey();
                System.out.println("c.key:"+key);
                System.out.println("c.element.value:"+c.element().getValue());
                String formatResult="";
                Iterable<String> processWithThatkey = c.element().getValue();
//                for(String p:processWithThatkey){
//                    System.out.println("p:"+p);
//                }
                formatResult=processWithThatkey+formatResult;
                //System.out.println("format:"+formatResult);
                c.output(KV.of(key,formatResult));
            }

        }));

        //results.apply(ParDo.of(new printString()));
        results.apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}
