import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class WordLength {

    //用于输出Pcollection中的字符串
    static class printString extends DoFn<String ,String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("c.element:" + c.element());
            c.output(c.element());
        }
    }

    static class printInteger extends DoFn<Integer ,Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("Integer:" + c.element());
            c.output(c.element());
        }
    }
    static class ComputeWordLengthFn extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c){
            String word = c.element();
            c.output(word.length());
        }
    }


    public static void main(String[] args) {

        //定义用于pipeline的options
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

//        //用命令行传入options
//        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
//        --<option>=<value>

        //创建pipeline
        Pipeline p = Pipeline.create(options);

        //创建Pcollection
        PCollection<String> words = p.apply(
                "ReadMyFile", TextIO.read().from("/home/maqy/Documents/beam_samples/output/WordLength"));
//        // Apply Create, passing the list and the coder, to create the PCollection.
//        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())

        //DoFn


        PCollection<Integer> wordLengths = words.apply(
                ParDo.of(new ComputeWordLengthFn()));

        //wordLengths.apply(ParDo.of(new printInteger()));


//        // Create a singleton PCollectionView from wordLengths using Combine.globally and View.asSingleton.
//        final PCollectionView<Integer> maxWordLengthCutOffView =
//                wordLengths.apply(Combine.globally(new MaxIntFn()).asSingletonView());
//
//
//
//        //p.apply(TextIO.write().to("wordcounts"));
//        PCollection<String> wordsBelowCutOff =
//                words.apply(ParDo
//                        .of(new DoFn<String, String>() {
//                            @ProcessElement
//                            public void processElement(ProcessContext c) {
//                                String word = c.element();
//                                // In our DoFn, access the side input.
//                                int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
//                                if (word.length() >= lengthCutOff) {
//                                    c.output(word);
//                                }
//                            }
//                        }).withSideInputs(maxWordLengthCutOffView)
//                );

        wordLengths.apply(ParDo.of(new printInteger()));
        //p.apply(TextIO.write().to("/home/maqy/Documents/beam_samples/"));

        // wordLengths.apply(MapElements.via());

        //wordLengths.apply("WriteToText", TextIO.write().to("/home/maqy/Documents/beam_samples/output/WordLengthOut"));


        p.run().waitUntilFinish();
    }
}
