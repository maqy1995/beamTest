import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

//Option设置
public interface OptionsSet extends PipelineOptions {
    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("/home/maqy/Documents/beam_samples/output/FlattenTest")
    //Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Validation.Required
    @Default.String("/home/maqy/文档/beam_samples/output/FlattenTestOut")
    String getOutput();

    void setOutput(String value);
}