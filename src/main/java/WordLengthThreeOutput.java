import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class WordLengthThreeOutput {


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> words = p.apply(
                "ReadMyFile", TextIO.read().from("/home/maqy/Documents/beam_samples/output/WordLengthThreeOutput"));
        // 输入PCollection中低于cutoff的单词发送给主输出PCollection<String>
        // 如果单词的长度大于cutoff，单词的长度发送给1个旁路输出PCollection<Integer>
        // 如果单词一"MARKER"开头, 将单词发送给旁路输出PCollection<String>  // ou
        final int wordLengthCutOff = 5;

        //为每个输出PCollection创建1个TupleTag
        // 单子低于cutoff长度的输出PCollection
        final TupleTag<String> wordsBelowCutOffTag =
                new TupleTag<String>() {
                };
        // 包含单词长度的输出PCollection
        final TupleTag<Integer> wordLengthsAboveCutOffTag =
                new TupleTag<Integer>() {
                };
        // 以"MARKER"开头的单词的输出PCollection
        final TupleTag<String> markedWordsTag =
                new TupleTag<String>() {
                };

// 将输出TupleTag传给ParDo
//调用.withOutputTags为每个输出指定TupleTag
// 先为主输出指定TupleTag，然后旁路输出
//在上边例子的基础上，为输出PCollection设定tag
// 所有的输出，包括主输出PCollection都被打包到PCollectionTuple中。
        PCollectionTuple results =
                words.apply(ParDo
                        .of(new DoFn<String, String>() {
                                //DoFn内的业务逻辑.
                                // 在ParDo的DoFn中，在调用ProcessContext.output的时候可以使用TupleTag指定将结果发送给哪个下游PCollection
// 在ParDo之后从PCollectionTuple中解出输出PCollection
// 在前边例子的基础上，本例示意了将结果输出到主输出和两个旁路输出
                                @ProcessElement
                                public void processElement(ProcessContext c) {
                                    String word = c.element();
                                    if (word.length() <= wordLengthCutOff) {
                                        // 将长度较短的单词发送到主输出
                                        // 在本例中，是wordsBelowCutOffTag代表的输出
                                        c.output(word);
                                    } else {
                                        // 将长度较长的单词发送到 wordLengthsAboveCutOffTag代表的输出中.
                                        c.output(wordLengthsAboveCutOffTag, word.length());
                                    }
                                    if (word.startsWith("MARKER")) {
                                        // 将以MARKER为开头的单词发送到markedWordsTag的输出中
                                        c.output(markedWordsTag, word);
                                    }
                                }
                            })
                        // 为主输出指定tag.
                        .withOutputTags(wordsBelowCutOffTag,
                                // 使用TupleTagList为旁路输出设定tag
                                TupleTagList.of(wordLengthsAboveCutOffTag)
                                        .and(markedWordsTag)));


        PCollection<String> wordsBelowCutOff = results.get(wordsBelowCutOffTag);
        PCollection<Integer> wordLengthsAboveCutOff = results.get(wordLengthsAboveCutOffTag);
        PCollection<String> makedWords = results.get(markedWordsTag);

        //wordsBelowCutOff.apply(ParDo.of(new Tools.printString()));
        //wordLengthsAboveCutOff.apply(ParDo.of(new Tools.printInteger()));
        makedWords.apply(ParDo.of(new Tools.printString()));

        p.run().waitUntilFinish();
    }
}
