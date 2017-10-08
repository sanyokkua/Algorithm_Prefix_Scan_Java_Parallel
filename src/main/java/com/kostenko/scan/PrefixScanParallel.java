package com.kostenko.scan;

import com.kostenko.scan.interfaces.Function;
import com.kostenko.scan.interfaces.PrefixScan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.arraycopy;

public class PrefixScanParallel implements PrefixScan<Integer> {
    private final int numberOfThreads;
    private final ExecutorService executorService;
    private final Lock lock;

    public PrefixScanParallel(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
        this.executorService = Executors.newFixedThreadPool(this.numberOfThreads);
        this.lock = new ReentrantLock();
    }

    @Override
    public List<Integer> computeSum(final Integer[] input, final Function<Integer, Integer> function) {
        final int size = input.length;
        final int correctedSize = findCorrectSize(size % 2 == 0 ? size + 1 : size);
        final int log2n = log2(correctedSize);
        final Integer[] x = new Integer[correctedSize];
        arraycopy(input, 0, x, 0, size);
        fillZeros(x, size);

        for (int d = 0; d <= log2n - 1; d++) {
            final int p2d1 = pow(2, d + 1);
            for (int k = 0; k < correctedSize - 1; k += p2d1) {
                x[k + pow(2, d + 1) - 1] = function.apply(x[k + pow(2, d) - 1], x[k + pow(2, d + 1) - 1]);
            }
        }
        x[correctedSize - 1] = 0;
        for (int d = log2n - 1; d >= 0; d--) {
            final int p2d1 = pow(2, d + 1);
            for (int k = 0; k < correctedSize - 1; k += p2d1) {
                int temp = x[k + pow(2, d) - 1];
                x[k + pow(2, d) - 1] = x[k + pow(2, d + 1) - 1];
                x[k + pow(2, d + 1) - 1] = function.apply(temp, x[k + pow(2, d + 1) - 1]);
            }
        }
        executorService.shutdown();
        List<Integer> result = new ArrayList<>();
        result.addAll(Arrays.asList(x).subList(1, size == correctedSize ? size : size + 1));
        return result;
    }

    private int findCorrectSize(final int size) {
        int result = size;
        if ((result & (result - 1)) == 0) {
            return result;
        }
        for (; (result & (result - 1)) != 0; result++) ;
        return result;

    }

    private synchronized int log2(int number) {
        return (int) (Math.log(number) / Math.log(2));
    }

    private synchronized int pow(int number, int pow) {
        return (int) Math.pow(number, pow);
    }

    private void fillZeros(Integer[] array, int size) {
        for (int i = size; i < array.length; i++) {
            array[i] = 0;
        }
    }
}