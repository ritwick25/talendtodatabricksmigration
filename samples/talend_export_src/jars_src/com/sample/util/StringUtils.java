package com.sample.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Synthetic shared helper library used by custom Java blocks across several
 * Talend sample jobs in this test-bed. Pure, deterministic, no external
 * dependencies -- safe to unit test standalone.
 */
public final class StringUtils {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^([^@]{1,64})@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$");

    private StringUtils() {
    }

    /**
     * Masks the local part of an email address, keeping the first and last
     * character visible, e.g. "jsmith@example.com" -> "j****h@example.com".
     * Returns the input unchanged if it does not look like an email.
     */
    public static String maskEmail(String email) {
        if (email == null) {
            return null;
        }
        Matcher m = EMAIL_PATTERN.matcher(email.trim());
        if (!m.matches()) {
            return email;
        }
        String local = m.group(1);
        String domain = m.group(2);
        if (local.length() <= 2) {
            return local.charAt(0) + "***@" + domain;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(local.charAt(0));
        for (int i = 1; i < local.length() - 1; i++) {
            sb.append('*');
        }
        sb.append(local.charAt(local.length() - 1));
        sb.append('@').append(domain);
        return sb.toString();
    }

    /** Collapses runs of whitespace (including tabs/newlines) into a single space and trims. */
    public static String normalizeWhitespace(String s) {
        if (s == null) {
            return null;
        }
        return s.trim().replaceAll("\\s+", " ");
    }

    /** Truncates a string to maxLen characters, appending an ellipsis if truncated. */
    public static String truncateWithEllipsis(String s, int maxLen) {
        if (s == null) {
            return null;
        }
        if (maxLen < 4) {
            throw new IllegalArgumentException("maxLen must be >= 4 to fit an ellipsis");
        }
        if (s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen - 3) + "...";
    }

    /** Title-cases a lower/upper mixed string on word boundaries (ASCII + common Unicode letters). */
    public static String titleCase(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        StringBuilder out = new StringBuilder(s.length());
        boolean startOfWord = true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c) || c == '-' || c == '\'') {
                startOfWord = true;
                out.append(c);
            } else if (startOfWord) {
                out.append(Character.toUpperCase(c));
                startOfWord = false;
            } else {
                out.append(Character.toLowerCase(c));
            }
        }
        return out.toString();
    }

    /** Safe left-pad with a given character, never truncates. */
    public static String padLeft(String s, int width, char padChar) {
        if (s == null) {
            s = "";
        }
        if (s.length() >= width) {
            return s;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < width - s.length(); i++) {
            sb.append(padChar);
        }
        sb.append(s);
        return sb.toString();
    }
}
