package com.sample.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Synthetic shared helper library (see StringUtils) -- date/time helpers
 * reused by Java blocks across several sample jobs. Uses the legacy
 * Date/Calendar API deliberately, to mirror what hand-written Talend
 * tJava/tJavaRow code typically looks like in real migrations.
 */
public final class DateUtil {

    private static final String[] CANDIDATE_PATTERNS = {
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "MM/dd/yyyy",
            "dd-MM-yyyy",
            "yyyyMMdd"
    };

    private DateUtil() {
    }

    /**
     * Attempts to parse a date string against several common patterns in
     * order, returning the first successful parse or null if none match.
     */
    public static Date parseFlexible(String s) {
        if (s == null || s.trim().isEmpty()) {
            return null;
        }
        String trimmed = s.trim();
        for (String pattern : CANDIDATE_PATTERNS) {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            sdf.setLenient(false);
            try {
                return sdf.parse(trimmed);
            } catch (ParseException e) {
                // try next pattern
            }
        }
        return null;
    }

    /** True if the given date is the last calendar day of its month. */
    public static boolean isMonthEnd(Date d) {
        if (d == null) {
            return false;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        return day == lastDay;
    }

    /** Adds N business days (skipping Saturday/Sunday) to a date. N may be negative. */
    public static Date addBusinessDays(Date d, int n) {
        if (d == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        int step = n >= 0 ? 1 : -1;
        int remaining = Math.abs(n);
        while (remaining > 0) {
            cal.add(Calendar.DAY_OF_MONTH, step);
            int dow = cal.get(Calendar.DAY_OF_WEEK);
            if (dow != Calendar.SATURDAY && dow != Calendar.SUNDAY) {
                remaining--;
            }
        }
        return cal.getTime();
    }

    /** Formats a date as ISO yyyy-MM-dd in the given timezone id (e.g. "UTC", "America/New_York"). */
    public static String formatIsoInZone(Date d, String timeZoneId) {
        if (d == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone(timeZoneId));
        return sdf.format(d);
    }

    /** True if the given year is a leap year (proleptic Gregorian rule). */
    public static boolean isLeapYear(int year) {
        return (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
    }
}
