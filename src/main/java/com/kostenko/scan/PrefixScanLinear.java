package com.kostenko.scan;

import com.kostenko.scan.interfaces.PrefixScan;

import java.util.function.BiFunction;

public class PrefixScanLinear implements PrefixScan<Integer> {
    @Override
    public Integer[] compute(Integer[] input, BiFunction<Integer, Integer, Integer> f) {
        validateInput(input, f);
        Integer[] result = new Integer[input.length];
        result[0] = input[0];
        for (int i = 1; i < input.length; i++) {
            result[i] = f.apply(result[i - 1], input[i]);
        }
        return result;
    }
}
