package com.kostenko.scan.interfaces;

import java.util.List;

public interface PrefixScan<T> {
    List<T> computeSum(T[] values, Function<T, T> function);
}
