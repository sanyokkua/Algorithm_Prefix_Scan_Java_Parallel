package com.kostenko.scan;

import com.kostenko.scan.interfaces.Function;
import com.kostenko.scan.interfaces.PrefixScan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

public class PrefixScanParallel implements PrefixScan<Integer> {
    private final ExecutorService executorService;
    private final Lock lock;

    public PrefixScanParallel(ExecutorService executorService) {
        this.executorService = executorService;
        this.lock = new ReentrantLock();
    }

    @Override
    public List<Integer> compute(final Integer[] input, final Function<Integer, Integer> f) {
        final int inputLength = input.length;
        final int resultLength = findPowerOf2Size(inputLength % 2 == 0 ? inputLength + 1 : inputLength);
        final int range = log2(resultLength) - 1;
        final Integer[] result = new Integer[resultLength];
        arraycopy(input, 0, result, 0, inputLength);
        fill(result, inputLength, resultLength, 0);

        for (int d = 0; d <= range; d++) {
            final int pow = pow(2, d + 1);
            for (int k = 0; k < resultLength - 1; k += pow) {
                result[k + pow(2, d + 1) - 1] = f.apply(result[k + pow(2, d) - 1], result[k + pow(2, d + 1) - 1]);
            }
        }

        result[resultLength - 1] = 0;
        for (int d = range; d >= 0; d--) {
            final int pow = pow(2, d + 1);
            for (int k = 0; k < resultLength - 1; k += pow) {
                int temp = result[k + pow(2, d) - 1];
                result[k + pow(2, d) - 1] = result[k + pow(2, d + 1) - 1];
                result[k + pow(2, d + 1) - 1] = f.apply(temp, result[k + pow(2, d + 1) - 1]);
            }
        }
        return toList(inputLength, resultLength, result);
    }

    private int findPowerOf2Size(final int size) {
        int result = size;
        if ((result & (result - 1)) == 0) {
            return result;
        }
        result--;
        result |= result >> 1;
        result |= result >> 2;
        result |= result >> 4;
        result |= result >> 8;
        result |= result >> 16;
        result++;
        return result;

    }

    private synchronized int log2(int number) {
        return (int) (Math.log(number) / Math.log(2));
    }

    private synchronized int pow(int number, int pow) {
        return (int) Math.pow(number, pow);
    }

    private List<Integer> toList(int size, int correctedSize, Integer[] x) {
        List<Integer> resultList = new ArrayList<>();
        resultList.addAll(Arrays.asList(x).subList(1, size == correctedSize ? size : size + 1));
        return resultList;
    }
}