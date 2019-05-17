package mobileGame.solution;

import mobileGame.GameActionInfo;
import mobileGame.utils.ExerciseOptions;
import mobileGame.utils.Input;
import mobileGame.utils.Output;
import myTest.JoinTest;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class StreamJoin {

    public static int num=1;
    // Extract user/score pairs from the event stream using processing time, via
    // global windowing.
    public static class UserLeaderBoard
            extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

        private Duration allowedLateness;
        private Duration updateFrequency;

        public UserLeaderBoard(Duration allowedLateness, Duration updateFrequency) {
            this.allowedLateness = allowedLateness;
            this.updateFrequency = updateFrequency;
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> input) {
            // [START EXERCISE 4 Part 1 - User Leaderboard]
            // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
            // Developer Docs: https://cloud.google.com/dataflow/model/triggering
            //
            // Compute the user scores since the beginning of time. To do this we will
            // need:
            //
            // 1. Use the GlobalWindows WindowFn so that all events occur in the same
            // window.
            // 2. Specify the accumulation mode so that total score is constantly
            // increasing.
            // 3. Trigger every updateFrequency duration
            //
            return input
                    .apply(Window
                            // Since we want a globally increasing sum, use the GlobalWindows
                            // WindowFn
                            .<GameActionInfo>into(new GlobalWindows())
                            // We want periodic results every updateFrequency of processing
                            // time. We will be triggering repeatedly and forever, starting
                            // updateFrequency after the first element seen. Window.
                            .triggering(Repeatedly.forever(
                                    AfterProcessingTime
                                            .pastFirstElementInPane()
                                            .plusDelayOf(updateFrequency)))
                            // Specify the accumulation mode to ensure that each firing of the
                            // trigger produces monotonically increasing sums rather than just
                            // deltas.
                            .accumulatingFiredPanes())

                    // Extract and sum username/score pairs from the event data.
                    // You can use the ExtractAndSumScore transform again.
                    // Name the step -- look at overloads of apply().
                    .apply("ExtractUserScore", new ExtractAndSumScore(GameActionInfo.KeyField.USER));
            // [END EXERCISE 4 Part 1]
        }
    }

    // Extract user/score pairs from the event stream using processing time, via
    // global windowing.
    public static class TeamLeaderBoard
            extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

        private Duration allowedLateness;
        private Duration earlyUpdateFrequency;
        private Duration lateUpdateFrequency;
        private Duration windowSize;

        public TeamLeaderBoard(Duration allowedLateness, Duration earlyUpdateFrequency, Duration lateUpdateFrequency,
                               Duration windowSize) {
            this.allowedLateness = allowedLateness;
            this.earlyUpdateFrequency = earlyUpdateFrequency;
            this.lateUpdateFrequency = lateUpdateFrequency;
            this.windowSize = windowSize;
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> input) {
            // [START EXERCISE 4 Part 2 - Team Leaderboard]
            // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
            // Developer Docs: https://cloud.google.com/dataflow/model/triggering
            //
            // We're going to produce windowed team score again, but this time we want
            // to get
            // early (speculative) results as well as occasional late updates.
            System.out.println("TeamLeaderBoard expand");
            return input
                    .apply("FixedWindows",
                            Window.<GameActionInfo>into(FixedWindows.of(windowSize))
                                    .triggering(AfterWatermark.pastEndOfWindow()
                                            // Specify .withEarlyFirings to produce speculative
                                            // results
                                            // with a delay of earlyUpdateFrequency
                                            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().alignedTo(earlyUpdateFrequency))
                                            // Specify .withLateFirings to produce late updates with a
                                            // delay
                                            // of lateUpdateFrequency
                                            .withLateFirings(AfterProcessingTime.pastFirstElementInPane().alignedTo(lateUpdateFrequency)))
                                    // Specify allowed lateness, and ensure that we get cumulative
                                    // results
                                    // across the window.
                                    .withAllowedLateness(allowedLateness).accumulatingFiredPanes())
                    // Extract and sum teamname/score pairs from the event data.
                    // You can use the ExtractAndSumScore transform again.
                    .apply("ExtractTeamScore", new ExtractAndSumScore(GameActionInfo.KeyField.TEAM));
            // [END EXERCISE 4 Part 2]
        }
    }

    /**
     * A transform to extract key/score information from GameActionInfo, and sum
     * the scores. The constructor arg determines whether 'team' or 'user' info is
     * extracted.
     */
    private static class ExtractAndSumScore
            extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

        private final GameActionInfo.KeyField field;

        ExtractAndSumScore(GameActionInfo.KeyField field) {
            this.field = field;
            System.out.println("ExtractAndSumScore field");
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> gameInfo) {
            System.out.println("ExtractAndSumScore expand");
            return gameInfo
                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                            .via((GameActionInfo gInfo) -> KV.of(field.extract(gInfo), gInfo.getScore())))
                    .apply(Sum.<String>integersPerKey());
        }
    }

    //用于输出到文件
    public static class FormatAsTextFn extends SimpleFunction<KV<String,Integer>, String> {
        @Override
        public String apply(KV<String,Integer> input) {
            return "key:"+input.getKey()+"   value:"+input.getValue();
        }
    }

    //用于输出Pcollection中的字符串
    static class printStringInteger extends DoFn<KV<String ,Integer> ,KV<String ,Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("c.element  StringInteger:" + c.element());
            c.output(c.element());
        }
    }

    static class printStringString extends DoFn<KV<String ,String> ,KV<String ,String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("c.element:" + c.element());
            c.output(c.element());
        }
    }
    static class printStringIntegerString extends DoFn<KV<String,KV<Integer,String>> ,KV<String,KV<Integer,String>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("joined.element:" + c.element());
            c.output(c.element());
        }
    }
    static class printStringIntegerInteger extends DoFn<KV<String,KV<Integer,Integer>> ,KV<String,KV<Integer,Integer>>> {
        public static int sum=0;
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("joined.StringIntegerInteger.element:" + c.element() +"sum:"+sum);
            sum++;
            c.output(c.element());
        }
    }

    //预处理，即将每行为 a,b De 的数据转化为KV<a,b>
    public static class Preprocess extends PTransform<PCollection<GameActionInfo>,PCollection<KV<String,Integer>>>{
        @Override
        public PCollection<KV<String,Integer>> expand(PCollection<GameActionInfo> gameActionInfoPCollection){
            //String[] temps = lines.toString().split(",");
            PCollection<KV<String,Integer>> result = gameActionInfoPCollection.apply(ParDo.of(new DoFn<GameActionInfo, KV<String,Integer>>() {
                @ProcessElement
                public void processElement(ProcessContext c){
                    String name = c.element().getUser();
                    Integer score = c. element().getScore();
                    KV<String ,Integer> kv=KV.of(name,score);
                    c.output(kv);
                }
            }));
            return result;
        }
    }
    private static final Duration ALLOWED_LATENESS = Duration.standardMinutes(30);
    private static final Duration EARLY_UPDATE_FREQUENCY = Duration.standardSeconds(10);
    private static final Duration LATE_UPDATE_FREQUENCY = Duration.standardSeconds(20);
    private static final Duration WINDOW_SIZE = Duration.standardSeconds(20);

    public static void main(String[] args) throws Exception {

        ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);
        //options.setStreaming(false);
        Pipeline pipeline = Pipeline.create(options);

        // Read game events from the unbounded injector.
        //如果不加入window是不能运行的

        PCollection<GameActionInfo> gameEvents = pipeline.apply(new Input.UnboundedGenerator())
                .apply(Window.<GameActionInfo>into(FixedWindows.of(WINDOW_SIZE)));

        //得到了流的PCollection<KV<String,Integer>>
        PCollection<KV<String,Integer>> stream = gameEvents.apply(new Preprocess());


        PCollection<GameActionInfo> gameEvents2 = pipeline.apply(new Input.BoundedGenerator())
                .apply(Window.<GameActionInfo>into(FixedWindows.of(WINDOW_SIZE)));

        //得到了流的PCollection<KV<String,Integer>>
        PCollection<KV<String,Integer>> stream2 = gameEvents2.apply(new Preprocess());

//        //Beam里头没有CountWindow么
//        PCollection<String> lines2=pipeline.apply(TextIO.read().from("/home/maqy/桌面/output/streamJoin"));
//
//        PCollection<KV<String,String>> batch=lines2.apply(new PTransform<PCollection<String>, PCollection<KV<String, String>>>() {
//            @Override
//            public PCollection<KV<String, String>> expand(PCollection<String> input) {
//                PCollection<KV<String,String>> result = input.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c){
//                        String[] s =c.element().split("\\W+");
//                        if(s.length==2){
//                            KV<String,String> kv =KV.of(s[0],s[1]);
//                            c.output(kv);
//                        }
//                    }
//                }));
//                return result;
//            }
//        });

        stream2.apply(ParDo.of(new printStringInteger()));
        //batch.apply(ParDo.of(new printStringString()));

        //stream.apply(ParDo.of(new printStringInteger()));
        //不允许  无界数据和有界数据进行join
        PCollection<KV<String,KV<Integer,Integer>>> joinedPcollection = Join.innerJoin(stream,stream2);

        joinedPcollection.apply(ParDo.of(new printStringIntegerInteger()));
        //PCollection<KV<String,Integer>> result =results.apply(ParDo.of(new printString()));

        pipeline.run();
    }
}