package com.kostenko.scan;

import com.kostenko.scan.interfaces.PrefixScan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import static com.kostenko.scan.utils.Utils.*;
import static java.lang.System.arraycopy;

public class PrefixScanParallel implements PrefixScan<Integer> {
    private final ExecutorService executorService;
    private final List<Future<?>> list;
    private final Lock lock;

    public PrefixScanParallel(ExecutorService executorService) {
        if (Objects.isNull(executorService)) {
            throw new IllegalArgumentException("ExecutorService is null");
        }
        this.executorService = executorService;
        this.lock = new ReentrantLock();
        this.list = new ArrayList<>();
    }

    @Override
    public Integer[] compute(final Integer[] input, final BiFunction<Integer, Integer, Integer> f) throws InterruptedException {
        validateInput(input, f);
        final int inputLength = input.length;
        final int resultLength = findPowerOf2Size(inputLength + 1);
        final int range = log2(resultLength) - 1;
        final int[] temporal = new int[resultLength];
        final int[] result = new int[inputLength];
        arraycopy(Arrays.stream(input).mapToInt(Integer::intValue).toArray(), 0, temporal, 0, inputLength);

        for (int d = 0; d <= range; d++) {
            final int D = d;
            final int pow = pow2(d + 1);
            for (int k = 0; k < resultLength - 1; k += pow) {
                final int K = k;
//                list.add(executorService.submit(() -> computeResultUp(f, temporal, D, K)));
                computeResultUp(f, temporal, D, K);
            }
//            futureSync(list);
        }
        temporal[resultLength - 1] = 0;
        for (int d = range; d >= 0; d--) {
            final int pow = pow2(d + 1);
            final int D = d;
            for (int k = 0; k < resultLength - 1; k += pow) {
                final int K = k;
//                list.add(executorService.submit(() -> computeResultDown(f, temporal, D, K)));
                computeResultDown(f, temporal, D, K);
            }
//            futureSync(list);
        }
        arraycopy(temporal, 1, result, 0, result.length);
        return Arrays.stream(result).sequential().boxed().toArray(Integer[]::new);
    }

    private void futureSync(List<Future<?>> list) {
        list.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        list.clear();
    }

    private void computeResultUp(BiFunction<Integer, Integer, Integer> f, int[] result, int d, int k) {
        lock.lock();
        try {
            result[k + pow2(d + 1) - 1] = f.apply(result[k + pow2(d) - 1], result[k + pow2(d + 1) - 1]);
        } finally {
            lock.unlock();
        }
    }

    private void computeResultDown(BiFunction<Integer, Integer, Integer> f, int[] result, int d, int k) {
        lock.lock();
        try {
            int temp = result[k + pow2(d) - 1];
            result[k + pow2(d) - 1] = result[k + pow2(d + 1) - 1];
            result[k + pow2(d + 1) - 1] = f.apply(temp, result[k + pow2(d + 1) - 1]);
        } finally {
            lock.unlock();
        }
    }
}