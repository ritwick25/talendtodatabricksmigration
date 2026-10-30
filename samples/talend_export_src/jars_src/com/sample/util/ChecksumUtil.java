package com.sample.util;

import java.util.zip.CRC32;

/**
 * Synthetic shared helper library (see StringUtils) -- numeric / checksum
 * helpers reused by Java blocks across several sample jobs.
 */
public final class ChecksumUtil {

    private ChecksumUtil() {
    }

    /** Returns the CRC32 checksum of the UTF-8 bytes of {@code data} as an 8-char uppercase hex string. */
    public static String crc32Hex(String data) {
        if (data == null) {
            data = "";
        }
        CRC32 crc = new CRC32();
        crc.update(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        long value = crc.getValue();
        return String.format("%08X", value);
    }

    /**
     * Standard Luhn checksum validation (used for card-number-shaped and other
     * check-digit identifiers in the fixtures). Input must be digits only.
     */
    public static boolean luhnCheck(String digitsOnly) {
        if (digitsOnly == null || !digitsOnly.matches("\\d+")) {
            return false;
        }
        int sum = 0;
        boolean alternate = false;
        for (int i = digitsOnly.length() - 1; i >= 0; i--) {
            int n = digitsOnly.charAt(i) - '0';
            if (alternate) {
                n *= 2;
                if (n > 9) {
                    n -= 9;
                }
            }
            sum += n;
            alternate = !alternate;
        }
        return sum % 10 == 0;
    }

    /**
     * A small deterministic weighted rolling checksum over an int array,
     * used as a stand-in "business checksum" for row-level reconciliation.
     * weight(i) = (i % 7) + 1, result wrapped into a signed 32-bit int via
     * explicit overflow handling so behavior is identical across JVMs.
     */
    public static long rollingChecksum(int[] values) {
        if (values == null) {
            return 0L;
        }
        long acc = 0L;
        for (int i = 0; i < values.length; i++) {
            long weight = (i % 7) + 1;
            acc += (long) values[i] * weight;
        }
        // fold into 32-bit range deterministically (mask, not a cast truncation)
        return acc & 0xFFFFFFFFL;
    }

    /** Epsilon-safe comparison for two doubles, default epsilon 1e-6. */
    public static boolean nearlyEqual(double a, double b) {
        return nearlyEqual(a, b, 1e-6);
    }

    public static boolean nearlyEqual(double a, double b, double epsilon) {
        return Math.abs(a - b) <= epsilon;
    }
}
