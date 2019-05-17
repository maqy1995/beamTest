import org.apache.beam.sdk.transforms.DoFn;

public class Tools {
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

}
