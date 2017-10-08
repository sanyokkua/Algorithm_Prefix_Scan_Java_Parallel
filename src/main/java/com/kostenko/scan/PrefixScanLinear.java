package com.kostenko.scan;

import com.kostenko.scan.interfaces.Function;
import com.kostenko.scan.interfaces.PrefixScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PrefixScanLinear implements PrefixScan<Integer> {
    @Override
    public List<Integer> computeSum(Integer[] input, Function<Integer, Integer> function) {
        if (Objects.isNull(input) || input.length == 0) {
            throw new IllegalArgumentException("List with input can't be empty");
        }
        List<Integer> result = new ArrayList<>();
        result.add(input[0]);
        for (int i = 1; i < input.length; i++) {
            result.add(function.apply(result.get(i - 1), input[i]));
        }
        return result;
    }
}
