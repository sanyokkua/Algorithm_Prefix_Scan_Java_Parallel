package com.kostenko.scan;

import com.kostenko.scan.interfaces.PrefixScan;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class GeneralTest {
    private static final BiFunction<Integer, Integer, Integer> PLUS = (first, second) -> first + second;
    private static final Integer[] testActual1 = new Integer[]{0, 1, 4, 6, 8, 11, 12, 13, 5, 7, 4, 9};
    private static final Integer[] testExpected1 = new Integer[]{0, 1, 5, 11, 19, 30, 42, 55, 60, 67, 71, 80};
    private static final Integer[] testActual2 = new Integer[]{1, 2, 3, 4, 5, 6};
    private static final Integer[] testExpected2 = new Integer[]{1, 3, 6, 10, 15, 21};
    private static final Integer[] testActual3 = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8};
    private static final Integer[] testExpected3 = new Integer[]{1, 3, 6, 10, 15, 21, 28, 36};
    private static final String DEFAULT_NUMBER_OF_RUNS = "100";
    private int numberOfRuns;
    private Integer[] testGeneratedInput_1_000;
    private Integer[] testGeneratedExpected_1_000;
    private Integer[] testGeneratedInput_10_000;
    private Integer[] testGeneratedExpected_10_000;
    private Integer[] testGeneratedInput_100_000;
    private Integer[] testGeneratedExpected_100_000;
    private Integer[] testGeneratedInput_1_000_000;
    private Integer[] testGeneratedExpected_1_000_000;

    @Before
    public void setup() throws Exception {
        String runs = System.getProperty("runs", DEFAULT_NUMBER_OF_RUNS);
        numberOfRuns = Integer.parseInt(runs);
        testGeneratedInput_1_000 = generateTestData(1_000);
        testGeneratedInput_10_000 = generateTestData(10_000);
        testGeneratedInput_100_000 = generateTestData(100_000);
        testGeneratedInput_1_000_000 = generateTestData(1_000_000);

        PrefixScan<Integer> prefixScanLinear = new PrefixScanLinear();

        testGeneratedExpected_1_000 = prefixScanLinear.compute(testGeneratedInput_1_000, PLUS);
        testGeneratedExpected_10_000 = prefixScanLinear.compute(testGeneratedInput_10_000, PLUS);
        testGeneratedExpected_100_000 = prefixScanLinear.compute(testGeneratedInput_100_000, PLUS);
        testGeneratedExpected_1_000_000 = prefixScanLinear.compute(testGeneratedInput_1_000_000, PLUS);
    }

    @Test
    public void testLinear() throws Exception {
        System.out.println("Begin linear tests:\n");
        PrefixScan<Integer> prefixScanLinear = new PrefixScanLinear();
        testData(prefixScanLinear, testActual1, testExpected1);
        testData(prefixScanLinear, testActual2, testExpected2);
        testData(prefixScanLinear, testActual3, testExpected3);
        testData(prefixScanLinear, testGeneratedInput_1_000, testGeneratedExpected_1_000);
        testData(prefixScanLinear, testGeneratedInput_10_000, testGeneratedExpected_10_000);
        testData(prefixScanLinear, testGeneratedInput_100_000, testGeneratedExpected_100_000);
        testData(prefixScanLinear, testGeneratedInput_1_000_000, testGeneratedExpected_1_000_000);
        System.out.println("Finish linear tests");
    }

    @Test
    public void testParallel() throws Exception {
        System.out.println("Begin parallel tests:\n");
        ExecutorService executorService = Executors.newWorkStealingPool(4);
        PrefixScan<Integer> prefixScanParallel = new PrefixScanParallel(executorService);
        testData(prefixScanParallel, testActual1, testExpected1);
        testData(prefixScanParallel, testActual2, testExpected2);
        testData(prefixScanParallel, testActual3, testExpected3);
        testData(prefixScanParallel, testGeneratedInput_1_000, testGeneratedExpected_1_000);
        testData(prefixScanParallel, testGeneratedInput_10_000, testGeneratedExpected_10_000);
        testData(prefixScanParallel, testGeneratedInput_100_000, testGeneratedExpected_100_000);
        testData(prefixScanParallel, testGeneratedInput_1_000_000, testGeneratedExpected_1_000_000);
        executorService.shutdown();
        System.out.println("Finish parallels tests");
    }

    private void testData(PrefixScan<Integer> prefixScan, Integer[] input, Integer[] expected) throws Exception {
        List<Long> resultOfRuns = new ArrayList<>(numberOfRuns);
        for (int i = 0; i < numberOfRuns; i++) {
            Instant before = Instant.now();
            List<Integer> actualList = Arrays.asList(prefixScan.compute(input, PLUS));
            Instant after = Instant.now();
            List<Integer> expectedList = Arrays.asList(expected);
            assertEquals(expectedList.size(), actualList.size());
            compareByElement(actualList, expectedList, Arrays.asList(input));
            resultOfRuns.add(Duration.between(before, after).toMillis());
        }
        long sum = resultOfRuns.stream().reduce((first, second) -> first + second).get();
        int size = input.length;
        long meanTime = sum / resultOfRuns.size();
        printResults(size, meanTime);
    }

    private void printResults(int size, long meanTime) {
        System.out.println("--------------------------------------------------------");
        System.out.println("Number of runs: " + numberOfRuns);
        System.out.println("Number of elements: " + size);
        System.out.println("Time: " + meanTime);
        System.out.println("--------------------------------------------------------");
    }

    private void compareByElement(List<Integer> actual, List<Integer> expected, List<Integer> input) {
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException("Sizes of Lists have to be equal");
        }
        //        System.out.println("Input:");
        //        System.out.println(input);
        //        System.out.println("Actual:");
        //        System.out.println(actual);
        //        System.out.println("Expected:");
        //        System.out.println(expected);
        //        System.out.println("\n");
        for (int i = 0; i < actual.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }

    private Integer[] generateTestData(int numberOfElements) {
        Random random = new Random(System.currentTimeMillis());
        Integer[] result = new Integer[numberOfElements];
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextInt();
        }
        return result;
    }
}
