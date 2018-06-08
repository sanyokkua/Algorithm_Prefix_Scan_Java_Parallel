package com.kostenko.scan;

import com.kostenko.scan.interfaces.PrefixScan;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import static com.kostenko.scan.utils.Utils.*;
import static java.lang.System.arraycopy;

public class PrefixScanParallel implements PrefixScan<Integer> {
    private final int size;
    private ExecutorService executorService;
    private boolean doInParallel;
    private int numberOfThreads;
    private int resultLength;
    private int[] temporal;
    private CountDownLatch countDownLatch;

    public PrefixScanParallel(boolean doInParallel) {
        this(doInParallel, 2);
    }

    public PrefixScanParallel(boolean doInParallel, int numberOfThreads) {
        this.doInParallel = doInParallel;
        this.numberOfThreads = numberOfThreads;
        this.size = resultLength / numberOfThreads;
    }

    @Override
    public Integer[] compute(final Integer[] input, final BiFunction<Integer, Integer, Integer> f) throws InterruptedException {
        validateInput(input, f);
        this.executorService = Executors.newWorkStealingPool(numberOfThreads);
        int inputLength = input.length;
        resultLength = findPowerOf2Size(inputLength + 1);
        int range = log2(resultLength) - 1;

        temporal = new int[resultLength];
        int[] result = new int[inputLength];

        arraycopy(Arrays.stream(input).mapToInt(Integer::intValue).toArray(), 0, temporal, 0, inputLength);

        for (int d = 0; d <= range; d++) {
            doInParallelUp(f, d);
        }
        temporal[resultLength - 1] = 0;

        for (int d = range; d >= 0; d--) {
            doInParallelDown(f, d);
        }

        arraycopy(temporal, 1, result, 0, result.length);
        executorService.shutdown();
        return Arrays.stream(result).sequential().boxed().toArray(Integer[]::new);
    }

    private void doInParallelUp(BiFunction<Integer, Integer, Integer> f, int d) throws InterruptedException {
        final int pow = pow2(d + 1);
        if (isDoInParallel(resultLength)) {
            Map<Integer, List<Integer>> map = generateK(pow, size);
            countDownLatch = new CountDownLatch(map.size());
            map.forEach((thread, listOfK) -> executorService.execute(() -> {
                listOfK.forEach(k -> computeResultUp(f, temporal, d, k));
                countDownLatch.countDown();
            }));
            countDownLatch.await();
        } else {
            for (int k = 0; k < resultLength - 1; k += pow) {
                computeResultUp(f, temporal, d, k);
            }
        }
    }

    private Map<Integer, List<Integer>> generateK(int pow, int size) {
        Map<Integer, List<Integer>> map = new HashMap<>();
        for (int i = 0, k = 0; i < numberOfThreads && k < resultLength - 1; i++) {
            List<Integer> tmp = new LinkedList<>();
            for (int j = 0; j < size && k < resultLength - 1; j++, k += pow) {
                tmp.add(k);
            }
            map.put(i, tmp);
        }
        return map;
    }

    private void doInParallelDown(BiFunction<Integer, Integer, Integer> f, int d) throws InterruptedException {
        final int pow = pow2(d + 1);
        if (isDoInParallel(resultLength)) {
            Map<Integer, List<Integer>> map = generateK(pow, size);
            countDownLatch = new CountDownLatch(map.size());
            map.forEach((thread, listOfK) -> executorService.execute(() -> {
                listOfK.forEach(k -> computeResultDown(f, temporal, d, k));
                countDownLatch.countDown();
            }));
            countDownLatch.await();
        } else {
            for (int k = 0; k < resultLength - 1; k += pow) {
                computeResultDown(f, temporal, d, k);
            }
        }
    }

    private boolean isDoInParallel(int arrayLength) {
        return arrayLength > 100 && doInParallel;
    }

    private void computeResultUp(BiFunction<Integer, Integer, Integer> f, int[] result, int d, int k) {
        result[k + pow2(d + 1) - 1] = f.apply(result[k + pow2(d) - 1], result[k + pow2(d + 1) - 1]);
    }

    private void computeResultDown(BiFunction<Integer, Integer, Integer> f, int[] result, int d, int k) {
        int temp = result[k + pow2(d) - 1];
        result[k + pow2(d) - 1] = result[k + pow2(d + 1) - 1];
        result[k + pow2(d + 1) - 1] = f.apply(temp, result[k + pow2(d + 1) - 1]);
    }
}