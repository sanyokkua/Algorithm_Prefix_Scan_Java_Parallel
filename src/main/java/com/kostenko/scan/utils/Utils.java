package com.kostenko.scan.utils;

public class Utils {

    public static int findPowerOf2Size(final int size) {
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

    public static int log2(int number) {
        return number == 0 ? 0 : 31 - Integer.numberOfLeadingZeros(number);
    }

    public static int pow2(int exponent) {
        return 1 << exponent;
    }
}
