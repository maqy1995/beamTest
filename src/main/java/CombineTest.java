import myTest.AverageFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;


public class CombineTest {
    //用于输出Pcollection中的字符串
    static class printString extends DoFn<Integer, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("c.element:" + c.element());
            c.output(c.element());
        }
    }

    //对每个元素进行的操作
    static class SetValue extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
           // System.out.println("c.element:" + c.element());
            Integer out = Integer.valueOf(c.element());
//            for(String temp:temps){
//                System.out.println(temp);
//            }

            c.output(out);
        }
    }

    //调用DoFn
    public static class Preprocess extends PTransform<PCollection<String>, PCollection<Integer>> {
        @Override
        public PCollection<Integer> expand(PCollection<String> lines) {
            //String[] temps = lines.toString().split(",");
            PCollection<Integer> result = lines.apply(ParDo.of(new SetValue()));
            return result;
        }

    }

    //通过Combine实现累加
    public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            int sum = 0;
            for (int item : input) {
                sum += item;
            }
            return sum;
        }
    }

    //用于输出到文本
    public static class FormatAsTextFn extends SimpleFunction<Double, String> {
        @Override
        public String apply(Double input) {
            return "sum=" + input.toString();
        }
    }

    //Option设置
    public interface CombineTestOptions extends PipelineOptions  {
        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("/home/maqy/Documents/beam_samples/output/CombineTest")
        //Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("/home/maqy/文档/beam_samples/output/CombineTestOut")
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) {
        OptionsSet options = PipelineOptionsFactory.fromArgs(args).withValidation().as(OptionsSet.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> lines = p.apply(TextIO.read().from("/home/maqy/Documents/beam_samples/output/CombineTest"));
        PCollection<Integer> preresult = lines.apply(new Preprocess());
        //preresult.apply(ParDo.of(new printString()));
       // PCollection<Integer> result = preresult.apply(Combine.globally(new SumInts()));
       // result.apply(ParDo.of(new printString()));
        PCollection<Double> result = preresult.apply(Combine.globally(new AverageFn()));


        result.apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}