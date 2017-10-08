package com.kostenko.scan.interfaces;

import java.util.List;

public interface PrefixScan<T> {
    List<T> compute(T[] values, Function<T, T> function);
}
