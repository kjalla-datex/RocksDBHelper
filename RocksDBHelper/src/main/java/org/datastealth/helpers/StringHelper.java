package org.datastealth.helpers;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.datastealth.helpers.log.Log;

import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringHelper {

    public static final String RANDOM_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()[]";
    public static final String RANDOM_ALPHA_NUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static final Map<String, Pattern> PATTERNS = new HashMap();

    public static final Pattern _handleReplacementSubstitution = Pattern.compile("\\$\\{(\\w+?)(\\[(!|#)*(\\w*?)\\])?\\}");
    public static final Pattern _removeAllSpaces = Pattern.compile("\\D");

    private StringHelper() {
        super();
    }

    public static boolean isNotBlank(String value) {
        return !isBlank(value, true);
    }

    public static boolean isNotBlank(String value, boolean trim) {
        return !isBlank(value, trim);
    }

    public static boolean isBlank(String value) {
        return isBlank(value, true);
    }

    public static boolean isBlank(String value, boolean trim) {
        return value == null || value.length() < 1 || trim && value.trim().length() < 1;
    }

    public static String defaultIfBlank(String value, String defaultValue) {
        if (isBlank(value, true)) {
            return defaultValue;
        }
        return value;
    }

    public static boolean matches(String compareTo, String value) {
        return matches(compareTo, value, "*");
    }

    public static boolean matches(String compareTo, String value, String wildcard) {
        return !(compareTo == null || value == null) && (compareTo.equals(wildcard) || compareTo.equals(value));
    }

    public static String unquoteString(String value) {
        if (value == null || value.length() < 2) {
            return value;
        }
        if (value.matches("\"(.*)\"") || value.matches("'(.*)'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static String[] splitWithQuotedStrings(String strParam, char token) {
        String str = strParam + token; // To detect last token when not quoted...
        ArrayList<String> strings = new ArrayList<>();
        boolean inQuote = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if ((c == '\"' || c == '\'') || c == token && !inQuote) {
                if (c == '\"' || c == '\'')
                    inQuote = !inQuote;
                if (!inQuote && sb.length() > 0) {
                    strings.add(sb.toString());
                    sb.delete(0, sb.length());
                }
            } else
                sb.append(c);
        }
        return strings.toArray(new String[strings.size()]);
    }

    public static boolean compare(String src, String target, boolean caseSensitive) {
        if (src == null && target == null) return true;
        if (src == null || target == null) return false;
        if (caseSensitive) {
            return src.equals(target);
        } else {
            return src.equalsIgnoreCase(target);
        }
    }


    public static Map<String, String> extractReplacements(Matcher matcher) {
        HashMap<String, String> retVal = new HashMap<>();
        for (int x = 0; x <= matcher.groupCount(); x++) {
            retVal.put(Integer.toString(x), matcher.group(x));
        }
        return retVal;
    }


    /**
     * Updates a replacement string, replacing all ${#} values with the corresponding groups from the parent matcher
     * <p>
     * For example, given a matcher source pattern "(\d)(\d)(\d)" and an input of 123, using a replacement string of
     * "A=${1},B=${2},C=${3}" will result in an output of "A=1,B=2,C=3".
     * <p>
     * Specifying an invalid or missing group (ie ${10}) will result in a blank and a logged warning, if failOnMissing is false.
     * <p>
     * Supports conditional processing.
     * Conditional processing is identified by a block [...] after the group name inside the ${}.
     * Options are
     * a) [#] - The size (string.length()) of the group name
     * b) [otherName] - This group name is included if the other name exists
     * c) [!othername] - This group is included if the other name does not exist
     * <p>
     * The following replaces "toPort" only if hello is not null
     * "${toPort[hello]}"
     * and you can apply only if hello is null using
     * ${toPort[!hello]}
     * <p>
     * If "A" = "123456789"
     * Then
     * ${A[#]}
     * will be replaced with
     * 9
     *
     * @param replacements  the source matcher to obtain the group substitutions from
     * @param replacement   the replacement format string to build
     * @param failOnMissing fail if a group is requested that is missing
     * @return the string after replacements
     */
    public static String handleReplacementSubstitution(Map<String, String> replacements, String replacement, boolean failOnMissing) {
        return handleReplacementSubstitution(replacements, replacement, failOnMissing, false);
    }

    public static String handleReplacementSubstitution(Map<String, String> replacements, String replacement, boolean failOnMissing, boolean allowSpecialCharsInName) {
        //if the replacement has groups and we have groups then we swap the values.  This allows
        //transferring values from the original match into the target value

        Matcher rm; // = Pattern.compile("\\$\\{(\\w+?-*\\w+?)(\\[(!|#)*(\\w*?)\\])?\\}").matcher(replacement);
        if(allowSpecialCharsInName) {
              rm = Pattern.compile("\\$\\{([a-zA-Z0-9_*-]+?)(\\[(!|#)*(\\w*?)\\])?\\}").matcher(replacement);
        } else {
            rm = _handleReplacementSubstitution.matcher(replacement);
        }

        //If the master pattern has groups, and the replacement string is requesting group replacement..
        if (replacements.size() > 0 && rm.find()) {
            StringBuilder replacementNew = new StringBuilder();
            int rmpos = 0;
            while (rm.find(rmpos)) {
                replacementNew.append(replacement.substring(rmpos, rm.start()));
                String grpName = rm.group(1);
                boolean replace = true;
                boolean count = false;

                //Check if we have extra options, and the extra options are not blank
                if (rm.groupCount() >= 3 && StringHelper.isNotBlank(rm.group(2))) {
                    boolean negate = false;
                    //this is an extra option using a ! to negate the option
                    if (rm.group(3) != null) {
                        if ("!".equals(rm.group(3))) negate = true;
                        if ("#".equals(rm.group(3))) count = true;
                    }
                    String referencedValue = rm.groupCount() >= 4 ? rm.group(4) : null;
                    if (referencedValue != null) {
                        String otherVal = replacements.get(referencedValue);
                        replace = count || (negate ? StringHelper.isBlank(otherVal) : StringHelper.isNotBlank(otherVal));
                    }
                }

                //If we are replacing
                if (replace) {
                    //and the key exists
                    if (replacements.containsKey(grpName)) {
                        String replaceWith;
                        if (count) {
                            int length = replacements.get(grpName) != null ? replacements.get(grpName).length() : 0;
                            replaceWith = Integer.toString(length);
                        } else {
                            replaceWith = replacements.get(grpName);
                        }
                        //and it's not null
                        if (replaceWith != null) replacementNew.append(replaceWith);
                    } else {
                        String msg = String.format("Request for a replacement value as group id %s, but the group does not exist! Target[%s]", grpName, replacement);
                        if (failOnMissing) {
                            throw new RuntimeException(msg);
                        }
                        Log.warn(msg);
                    }
                }
                rmpos = rm.end();
            }
            if (rmpos < replacement.length()) {
                replacementNew.append(replacement.substring(rmpos, replacement.length()));
            }
            return replacementNew.toString();
        }
        return replacement;
    }

    /**
     * Helper function to check if a value matches or contains a pattern.  This will ignore a regex pattern match
     * issue and try a "contains" check
     *
     * @param matchPattern the pattern or string to check the value for
     * @param value        the value to be checked
     * @return true if the pattern matches or is found in the value
     */
    public static boolean isRegexOrNameMatch(String matchPattern, String value, boolean emptyAsMatch) {
        if (StringHelper.isBlank(matchPattern)) return emptyAsMatch;

        boolean isMatch = StringHelper.isBlank(matchPattern);
        Pattern pattern = PATTERNS.get(matchPattern);
        if (pattern == null) {
            pattern = Pattern.compile(matchPattern);
            PATTERNS.put(matchPattern, pattern);
        }
        try {
            isMatch = isMatch || pattern.matcher(value).matches();
        } catch (Exception e) {
            Log.debug("Exception caught matching pattern: pattern:%s value:%s error:%s", pattern, value, e);
            //Not a regex??
        }
        return isMatch || value.contains(matchPattern);
    }


    /**
     * Parse a string that is composed of int lengths + seperator + chars[length = header length)
     * For example, given
     * 5;AAAAA5;BBBBB;5CCCCC
     * this would return a list containing AAAAA, BBBBB and CCCCC
     *
     * @param data      the string to parse
     * @param seperator the character seperator
     * @return a list of parsed strings. If a string length is missing this will return the whole string
     */
    public static List<String> parseLengthDefinedStrings(String data, String seperator) {
        String input = data;
        List<String> retVal = new ArrayList<>();

        while (input.length() > 0) {
            String pattern = "^(?s)(\\d+)" + Pattern.quote(seperator) + "(.*)$";
            Matcher m = Pattern.compile(pattern).matcher(input);
            if (!m.matches()) {
                //No match add what we have left and exit
                retVal.add(input);
                return retVal;
            } else {
                //Match, get the length
                int length = NumberHelper.parseInteger(m.group(1), 0);
                if (length <= 0) {
                    //Invalid value, return the remaining text
                    retVal.add(m.group(2));
                    return retVal;
                } else {
                    input = m.group(2);
                    String thisVal = StringUtils.substring(input, 0, length);
                    input = StringUtils.substring(input, length, input.length());
                    retVal.add(thisVal);
                }
            }
        }
        return retVal;
    }



    public static String generateRandom() {
        return generateRandom(32);
    }

    public static String generateRandom(int size) {
        return generateRandom(size, RANDOM_CHARACTERS);
    }

    public static String generateRandom(int size, String randomCharacters) {
        return RandomStringUtils.random(size, randomCharacters.toCharArray());
    }

    public static String removeAllSpaces(String input) {
        return _removeAllSpaces.matcher(input).replaceAll("");
    }

    @SuppressWarnings("squid:CallToDeprecatedMethod") //
    public static String urlEncode(String input, String encoding) {
        String returnValue = input;
        try {
            returnValue = URLEncoder.encode(input, encoding);
        }
        catch (Throwable e) {
            Log.debug("Exception caught encoding input:%s encoding:%s error:%s", input, encoding, e);
        }
        return returnValue;
    }

    public static Map<String, Integer> getNamedGroups(String pattern) {
        if (pattern == null) {
            return null;
        }

        Pattern p = Pattern.compile(pattern);
        Map<String, Integer> returnValue = null;
        try {
            Method m = p.getClass().getDeclaredMethod("namedGroups");
            if (m != null) {
                m.setAccessible(true);
                returnValue = (Map<String, Integer>)m.invoke(p);
            }
        }
        catch (Throwable t) {
            Log.debug("Cannot get named group:{%s}", t);
        }
        return returnValue;
    }

    public static Map<String, String> getNamedValues(String valuePattern, String value) {
        if (valuePattern == null) {
            return null;
        }

        Map<String, String> returnValue = new HashMap<>();
        try {
            Pattern p = Pattern.compile(valuePattern);
            Matcher matcher = p.matcher(value);
            if (matcher.matches()) {
                Map<String, Integer> namedGroups = getNamedGroups(valuePattern);
                if (namedGroups != null) {
                    for (String groupName : namedGroups.keySet()) {
                        String groupValue = matcher.group(groupName);
                        if (StringHelper.isNotBlank(groupValue)) {
                            returnValue.put(groupName, groupValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            Log.debug("Exception caught matching pattern: error:%s", e);
        }
        return returnValue;
    }

    public static Map<String, Integer> getNamedGroupsForPattern(Pattern valuePattern) {
        if (valuePattern == null) {
            return null;
        }

        Pattern p = valuePattern;
        Map<String, Integer> returnValue = null;
        try {
            Method m = p.getClass().getDeclaredMethod("namedGroups");
            if (m != null) {
                m.setAccessible(true);
                returnValue = (Map<String, Integer>)m.invoke(p);
            }
        }
        catch (Throwable t) {
            Log.debug("Cannot get named group:{%s}", t);
        }
        return returnValue;
    }

    public static Map<String, String> getNamedValuesForPattern(Pattern valuePattern, String value) {
        if (valuePattern == null) {
            return null;
        }

        Map<String, String> returnValue = new HashMap<>();
        try {
            Matcher matcher = valuePattern.matcher(value);
            if (matcher.matches()) {
                Map<String, Integer> namedGroups = getNamedGroupsForPattern(valuePattern);
                if (namedGroups != null) {
                    for (String groupName : namedGroups.keySet()) {
                        String groupValue = matcher.group(groupName);
                        if (StringHelper.isNotBlank(groupValue)) {
                            returnValue.put(groupName, groupValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            Log.debug("Exception caught matching pattern: error:%s", e);
        }
        return returnValue;
    }



    public static String replace(String value, String valuePattern, String replacement) {
        if (StringHelper.isBlank(valuePattern) || StringHelper.isBlank(replacement)) {
            return value;
        }
        Map<String, String> namedValues = getNamedValues(valuePattern, value);
        String returnValue = handleReplacementSubstitution(namedValues, replacement, false);
        if (returnValue.equals(replacement)) {
            returnValue = value;
        }
        return returnValue;
    }
}
