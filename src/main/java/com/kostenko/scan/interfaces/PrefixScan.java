package com.kostenko.scan.interfaces;

import java.util.Objects;
import java.util.function.BiFunction;

public interface PrefixScan<T> {
    T[] compute(T[] values, BiFunction<T, T, T> function) throws Exception;

    default void validateInput(T[] values, BiFunction<T, T, T> function) {
        if (Objects.isNull(values) || values.length == 0 || Objects.isNull(function)) {
            throw new IllegalArgumentException("Input or function is invalid");
        }
    }
}
