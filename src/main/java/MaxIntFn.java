import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.transforms.Combine;

//计算最大值
public class MaxIntFn extends Combine.CombineFn<Integer,Integer,Integer>{
    public static Integer max=0;

    @Override
    public Integer createAccumulator(){
        return new Integer(0);
    }

    @Override
    public Integer addInput(Integer max,Integer input) {
        if(input>max)
            return input;
        else
            return max;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> integers) {
        Integer merged=createAccumulator();
        for(Integer integer:integers) {
            if(integer>merged) {
                merged=integer;
            }
        }
        return merged;
    }

    @Override
    public Integer extractOutput(Integer max) {
        return max;
    }
}



