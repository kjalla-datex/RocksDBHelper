package org.datastealth.helpers;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.regex.Pattern;


public class NumberHelper {

    protected static final String[] SI_UNITS = new String[]{"", "k", "M", "G", "T", "P", "E", "Z", "Y"};
    protected static final String[] SI_UNITS_LONG = {"", "Thousand", "Million", "Billion", "Trillion", "Quadrillion", "Quintillion", "Sextillion", "Septillion"};
    protected static final String[] BYTE_UNITS = new String[]{"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
    protected static final String[] BYTE_UNITS_LONG = {"bytes", "Kilobyte", "Megabyte", "Gigabyte", "Terabyte", "Petabyte", "Exabyte", "Zettabyte", "Yottabyte"};

    private NumberHelper() {
        //Hide Constructor
    }

    public static int parseInteger(String value, int defaultValue) {
        try {
            BigDecimal val = parseNumberSI(value);
            return val.intValue();
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static BigDecimal parseNumberSI(String value) {
        return parseNumberString(value, SI_UNITS, 1000);
    }

    public static BigDecimal parseNumberBytes(String value) {
        return parseNumberString(value, BYTE_UNITS, 1024);
    }

    private static Pattern matchAllDigits = Pattern.compile("[\\d]+");

    public static BigDecimal parseNumberString(String value, String[] units, int base) {
        //Null/blank is zero
        if (value == null || value.length() < 1) {
            return BigDecimal.ZERO;
        }
        //Contains all digits, just parse it
//        if (value.replaceAll("\\d", "").trim().length() == 0) {
//            return new BigDecimal(value);
//        }
        if (matchAllDigits.matcher(value).replaceAll("").trim().length() == 0) {
            return new BigDecimal(value);
        }

        //Check for scientific notation
        if (value.matches("[\\d\\.]*e\\d*")) {
            String[] parts = value.split("e");
            BigDecimal retVal = new BigDecimal(parts[0]);
            int pow = Integer.parseInt(parts[1]);
            return retVal.multiply(new BigDecimal(10).pow(pow));
        }

        //parse the numeric part
        String nums = value.replaceAll("[^-\\d\\.]", "");
        BigDecimal retVal = new BigDecimal(nums);

        //figure out if we need to multiple, strip all the numbers and whitespace
        String chars = value.replaceAll("[-\\d\\.\\s]", "");
        double multi = 1;
        for (int x = 0; x < units.length; x++) {
            if (chars.equalsIgnoreCase(units[x])) {
                multi = Math.pow(base, x);
                break;
            }
        }
        return retVal.multiply(BigDecimal.valueOf(multi));
    }

    public static String formatNumberSI(Number value) {
        return formatNumberSI(value, true);
    }

    public static String formatNumberSI(Number value, boolean abbreviate) {
        if (value == null) return "0";

        if (Math.abs(value.doubleValue()) < 999) {
            return value.toString();
        }
        final String[] units = abbreviate ? SI_UNITS : SI_UNITS_LONG;
        int digitGroups = (int) (Math.log10(value.doubleValue()) / Math.log10(1000));
        return new DecimalFormat("#,##0.#").format(value.doubleValue() / Math.pow(1000, digitGroups)) + " " + units[digitGroups];

    }

    public static String formatSizeBytes(Number size) {
        return formatSizeBytes(size, true);
    }

    public static String formatSizeBytes(Number size, boolean abbreviate) {
        if (size == null) return "0";

        if (size.doubleValue() <= 0) return "0";
        final String[] units = abbreviate ? BYTE_UNITS : BYTE_UNITS_LONG;
        int digitGroups = (int) (Math.log10(size.doubleValue()) / Math.log10(1024));
        if (digitGroups < 0) digitGroups = 0;
        return new DecimalFormat("#,##0.#").format(size.doubleValue() / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    private static Pattern matchAllNonDigits = Pattern.compile("[\\D]+");

    public static boolean isValidLuhn(String valueParam) {
        //http://en.wikipedia.org/wiki/Luhn_algorithm
        //value = value.replaceAll("\\D", "");
        String value = matchAllNonDigits.matcher(valueParam).replaceAll("");
        if (value.trim().length() < 1) {
            return false;
        }

        int checksum = generateLuhnDigit(value.substring(0, value.length() - 1));
        char c = value.charAt(value.length() - 1);
        int val = NumberHelper.parseInteger(Character.toString(c), -1);
        return val == checksum;
    }

    public static int generateLuhnDigit(String valueParam) {
        //value = value.replaceAll("\\D", "");
        String value = matchAllNonDigits.matcher(valueParam).replaceAll("");
        int sum = 0;
        if (value.trim().length() < 1) {
            return 0;
        }

        int pos = 0;
        for (int x = value.length() - 1; x > -1; x--) {
            pos++;
            char c = value.charAt(x);
            int num = NumberHelper.parseInteger(Character.toString(c), -1);
            if (num < 0) {
                return 0;
            }
            if (pos % 2 == 1) {
                int mult = num * 2;
                if (mult > 9) {
                    sum = sum + (mult % 10) + 1;
                } else {
                    sum = sum + mult;
                }
            } else {
                sum = sum + num;
            }
        }
        return (sum % 10 == 0) ? 0 : 10 - (sum % 10);
    }
}
