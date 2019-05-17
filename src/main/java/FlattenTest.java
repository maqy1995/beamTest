
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenTest {
    //用于输出Pcollection中的字符串
    static class printString extends DoFn<String ,String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("c.element:" + c.element());
            c.output(c.element());
        }
    }
    public static void main(String[] args){
//        static final List<String> LINES = Arrays.asList(
//                "To be, or not to be: that is the question: ",
//                "Whether 'tis nobler in the mind to suffer ",
//                "The slings and arrows of outrageous fortune, ",
//                "Or to take arms against a sea of troubles, ");

        String s1="abcdefg";

        OptionsSet options = PipelineOptionsFactory.fromArgs(args).withValidation().as(OptionsSet.class);
        Pipeline p = Pipeline.create(options);

        //PCollection<String> lines = p.apply(TextIO.read().from("/home/maqy/Documents/beam_samples/output/FlattenTest"));
        //PCollection<Integer> preresult = lines.apply(new CombineTest.Preprocess());

        //直接赋值字符串
        PCollection<String> l1=p.apply(Create.of(s1));
        PCollection<String> l2=p.apply(Create.of("hijklm"));
        PCollection<String> l3=p.apply(Create.of("opqrs"));

        //读取文件获得字符串
        PCollection<String> line1 = p.apply(TextIO.read().from("/home/maqy/Documents/beam_samples/output/Flatten1"));
        PCollection<String> line2 = p.apply(TextIO.read().from("/home/maqy/Documents/beam_samples/output/Flatten2"));
        PCollection<String> line3 = p.apply(TextIO.read().from("/home/maqy/Documents/beam_samples/output/Flatten3"));
       // l1.apply(ParDo.of(new printString()));
        PCollectionList<String> collections= PCollectionList.of(line1).and(line2).and(line3);
        PCollection<String> merged =collections.apply(Flatten.<String>pCollections());


        merged.apply(ParDo.of(new printString()));

       // merged.apply(TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}
