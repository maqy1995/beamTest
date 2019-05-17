package myTest;

/**
 * Beam中的Join测试
 * Beam版本：2.3
 *
 * @author：maqy
 * @date：2018.09.22
 */

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class JoinTest {
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

    //用于输出到文件
    public static class FormatAsTextFn extends SimpleFunction<KV<String,KV<String,String>>, String> {
        @Override
        public String apply(KV<String,KV<String,String>> input) {
            return "key:"+input.getKey()+"   value:"+input.getValue();
        }
    }

    public interface JoinTestOptions extends PipelineOptions,StreamingOptions {
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
        JoinTestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JoinTestOptions.class);
        options.setStreaming(true); //通过这里可以显示的指定批或流
        Pipeline p= Pipeline.create(options);

        //第一个文件
        PCollection<String> lines1=p.apply(TextIO.read().from("/home/maqy/桌面/output/BeamJoin1"));
        //第二个文件
        PCollection<String> lines2=p.apply(TextIO.read().from("/home/maqy/桌面/output/BeamJoin2"));
        //预处理，即将每行为 a,b De 的数据转化为KV<a,b>
        PCollection<KV<String,String>> leftPcollection=lines1.apply(new Preprocess());
        PCollection<KV<String,String>> rightPcollection=lines2.apply(new Preprocess());

        PCollection<KV<String,KV<String,String>>> joinedPcollection = Join.innerJoin(leftPcollection,rightPcollection);
//
//        //results.apply(ParDo.of(new printString()));
        joinedPcollection.apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to("/home/maqy/桌面/output/BeamJoinout1"));

        p.run().waitUntilFinish();
    }
}

