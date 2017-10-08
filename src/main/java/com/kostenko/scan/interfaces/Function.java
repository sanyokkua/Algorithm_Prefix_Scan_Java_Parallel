package com.kostenko.scan.interfaces;

public interface Function<T, R> {
    R apply(final T first, final T second);
}
