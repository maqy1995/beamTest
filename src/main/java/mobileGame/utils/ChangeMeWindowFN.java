package mobileGame.utils;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;

import java.util.Collection;

public class ChangeMeWindowFN<T, W extends BoundedWindow> extends WindowFn<T, W> {

  @Override
  public Collection<W> assignWindows(AssignContext arg0) throws Exception {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> arg0) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void mergeWindows(MergeContext arg0) throws Exception {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Coder<W> windowCoder() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public WindowMappingFn<W> getDefaultWindowMappingFn() {
    throw new RuntimeException("Not implemented");
  }
}
