package myTest;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine;

//计算平均值
public class AverageFn extends Combine.CombineFn<Integer, AverageFn.Accum, Double> {
    @DefaultCoder(AvroCoder.class)
    public static class Accum {
        int sum = 0;
        int count = 0;
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Integer input) {
        accum.sum += input;
        accum.count++;
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.sum += accum.sum;
            merged.count += accum.count;
        }
        return merged;
    }

    @Override
    public Double extractOutput(Accum accum) {
        return ((double) accum.sum) / accum.count;
    }
}