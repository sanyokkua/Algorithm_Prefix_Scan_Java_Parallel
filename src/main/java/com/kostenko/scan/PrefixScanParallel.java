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
    private ExecutorService executorService;
    private boolean doInParallel;
    private int numberOfThreads;
    private int resultArrayLength;
    private int[] tempResultArray;
    private CountDownLatch countDownLatch;

    public PrefixScanParallel() {
        this(false, 1);
    }

    public PrefixScanParallel(boolean doInParallel, int numberOfThreads) {
        this.doInParallel = doInParallel;
        this.numberOfThreads = numberOfThreads;
    }

    @Override
    public Integer[] compute(final Integer[] input, final BiFunction<Integer, Integer, Integer> f) throws InterruptedException {
        validateInput(input, f);
        this.executorService = Executors.newWorkStealingPool(numberOfThreads);
        int inputArrayLength = input.length;
        resultArrayLength = findPowerOf2Size(inputArrayLength + 1);
        int range = log2(resultArrayLength) - 1;
        tempResultArray = new int[resultArrayLength];
        int[] resultArray = new int[inputArrayLength];

        arraycopy(Arrays.stream(input).mapToInt(Integer::intValue).toArray(), 0, tempResultArray, 0, inputArrayLength);

        for (int d = 0; d <= range; d++) {
            doInParallel(f, d, computeUp);
        }
        tempResultArray[resultArrayLength - 1] = 0;
        for (int d = range; d >= 0; d--) {
            doInParallel(f, d, computeDown);
        }

        arraycopy(tempResultArray, 1, resultArray, 0, resultArray.length);
        executorService.shutdown();
        return Arrays.stream(resultArray).sequential().boxed().toArray(Integer[]::new);
    }

    private void doInParallel(BiFunction<Integer, Integer, Integer> f, int d, Compute computeFunction) throws InterruptedException {
        final int pow = pow2(d + 1);
        if (doInParallel) {
            countDownLatch = new CountDownLatch(numberOfThreads);
            final int arraySizePerThread = resultArrayLength / numberOfThreads;
            int boundPerThread = arraySizePerThread;
            int numberOfKPerThread = boundPerThread / pow;
            int K = 0;
            int powStep = numberOfKPerThread == 0 ? pow : pow * numberOfKPerThread;
            for (int threadId = 0; threadId < numberOfThreads; threadId++) {
                final int finalK = K;
                final int finalBound = boundPerThread;
                executorService.execute(() -> {
                    int k = finalK;
                    do {
                        if (k < resultArrayLength - 1) {
                            computeFunction.compute(f, tempResultArray, d, k);
                            k += pow;
                        }
                    } while ((k < finalBound) && k < resultArrayLength - 1);
                    countDownLatch.countDown();
                });
                boundPerThread += arraySizePerThread;
                K += powStep;
            }
            countDownLatch.await();
        } else {
            for (int k = 0; k < resultArrayLength - 1; k += pow) {
                computeFunction.compute(f, tempResultArray, d, k);
            }
        }
    }

    private final Compute computeUp = (f, result, d, k) -> result[k + pow2(d + 1) - 1] = f.apply(result[k + pow2(d) - 1], result[k + pow2(d + 1) - 1]);

    private final Compute computeDown = (f, result, d, k) -> {
        int temp = result[k + pow2(d) - 1];
        result[k + pow2(d) - 1] = result[k + pow2(d + 1) - 1];
        result[k + pow2(d + 1) - 1] = f.apply(temp, result[k + pow2(d + 1) - 1]);
    };

    private interface Compute {
        void compute(BiFunction<Integer, Integer, Integer> f, int[] result, int d, int k);
    }
}