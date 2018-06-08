package com.kostenko.scan;

import com.kostenko.scan.interfaces.PrefixScan;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class GeneralTest {
    private static final BiFunction<Integer, Integer, Integer> PLUS = (first, second) -> first + second;
    private static final Integer[] testActual1 = new Integer[]{0, 1, 4, 6, 8, 11, 12, 13, 5, 7, 4, 9};
    private static final Integer[] testExpected1 = new Integer[]{0, 1, 5, 11, 19, 30, 42, 55, 60, 67, 71, 80};
    private static final Integer[] testActual2 = new Integer[]{1, 2, 3, 4, 5, 6};
    private static final Integer[] testExpected2 = new Integer[]{1, 3, 6, 10, 15, 21};
    private static final Integer[] testActual3 = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8};
    private static final Integer[] testExpected3 = new Integer[]{1, 3, 6, 10, 15, 21, 28, 36};
    private int numberOfRuns;
    private Integer[] testGeneratedInput_1_000;
    private Integer[] testGeneratedExpected_1_000;
    private Integer[] testGeneratedInput_10_000;
    private Integer[] testGeneratedExpected_10_000;
    private Integer[] testGeneratedInput_100_000;
    private Integer[] testGeneratedExpected_100_000;
    private Integer[] testGeneratedInput_1_000_000;
    private Integer[] testGeneratedInput_1_000_000_0;
    private Integer[] testGeneratedExpected_1_000_000;
    private Integer[] testGeneratedExpected_1_000_000_0;

    @Before
    public void setup() throws Exception {
        String runs = System.getProperty("runs", "100");
        numberOfRuns = Integer.parseInt(runs);
        testGeneratedInput_1_000 = generateTestData(1_000);
        testGeneratedInput_10_000 = generateTestData(10_000);
        testGeneratedInput_100_000 = generateTestData(100_000);
        testGeneratedInput_1_000_000 = generateTestData(1_000_000);
        testGeneratedInput_1_000_000_0 = generateTestData(1_000_000_0);
        System.out.println("Data generated");

        PrefixScan<Integer> prefixScanLinear = new PrefixScanLinear();

        testGeneratedExpected_1_000 = prefixScanLinear.compute(testGeneratedInput_1_000, PLUS);
        testGeneratedExpected_10_000 = prefixScanLinear.compute(testGeneratedInput_10_000, PLUS);
        testGeneratedExpected_100_000 = prefixScanLinear.compute(testGeneratedInput_100_000, PLUS);
        testGeneratedExpected_1_000_000 = prefixScanLinear.compute(testGeneratedInput_1_000_000, PLUS);
        testGeneratedExpected_1_000_000_0 = prefixScanLinear.compute(testGeneratedInput_1_000_000_0, PLUS);
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
    public void testParallelOff() throws Exception {
        System.out.println("Begin parallel off tests:\n");
        PrefixScan<Integer> prefixScanParallel = new PrefixScanParallel();
        testData(prefixScanParallel, testActual1, testExpected1);
        testData(prefixScanParallel, testActual2, testExpected2);
        testData(prefixScanParallel, testActual3, testExpected3);
        testData(prefixScanParallel, testGeneratedInput_1_000, testGeneratedExpected_1_000);
        testData(prefixScanParallel, testGeneratedInput_10_000, testGeneratedExpected_10_000);
        testData(prefixScanParallel, testGeneratedInput_100_000, testGeneratedExpected_100_000);
        testData(prefixScanParallel, testGeneratedInput_1_000_000, testGeneratedExpected_1_000_000);
        testData(prefixScanParallel, testGeneratedInput_1_000_000_0, testGeneratedExpected_1_000_000_0);
        System.out.println("Finish parallels off tests");
    }

    @Test
    public void testParallelOn() throws Exception {
        System.out.println("Begin parallel on tests:\n");
        PrefixScan<Integer> prefixScanParallel = new PrefixScanParallel(true, 4);
        testData(prefixScanParallel, testActual1, testExpected1);
        testData(prefixScanParallel, testActual2, testExpected2);
        testData(prefixScanParallel, testActual3, testExpected3);
        testData(prefixScanParallel, testGeneratedInput_1_000, testGeneratedExpected_1_000);
        testData(prefixScanParallel, testGeneratedInput_10_000, testGeneratedExpected_10_000);
        testData(prefixScanParallel, testGeneratedInput_100_000, testGeneratedExpected_100_000);
        testData(prefixScanParallel, testGeneratedInput_1_000_000, testGeneratedExpected_1_000_000);
        testData(prefixScanParallel, testGeneratedInput_1_000_000_0, testGeneratedExpected_1_000_000_0);
        System.out.println("Finish parallels on tests");
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
        long timeMin = resultOfRuns.stream().reduce(BinaryOperator.minBy(Long::compareTo)).get();
        long timeMax = resultOfRuns.stream().reduce(BinaryOperator.maxBy(Long::compareTo)).get();
        double timeAverage = resultOfRuns.stream().mapToLong(Long::longValue).average().getAsDouble();
        String prefixScanName = prefixScan.getClass().getSimpleName();
        printResults(timeMin, timeMax, timeAverage, input.length, prefixScanName);
    }

    private void printResults(long timeMin, long timeMax, double timeAvg, int size, String prefixScanName) {
        System.out.println("--------------------------------------------------------");
        System.out.println(format("Running by: %s, number of runs: %d, size: %d", prefixScanName, numberOfRuns, size));
        System.out.println(format("Time: min - %d millis, max - %d millis, avg %s millis", timeMin, timeMax, timeAvg));
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
