import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class TimeStampTest {
    public static void main(String[] args){
        OptionsSet options = PipelineOptionsFactory.fromArgs(args).withValidation().as(OptionsSet.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> lines = p.apply(TextIO.read().from("/home/maqy/Documents/beam_samples/output/TimeStampTest"));


    }
}
