package edu.uci.ics.cs221.analysis;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Project 1, task 1: Implement a simple tokenizer based on punctuations and white spaces.
 *
 * For example: the text "I am Happy Today!" should be tokenized to ["happy", "today"].
 *
 * Requirements:
 *  - White spaces (space, tab, newline, etc..) and punctuations provided below should be used to tokenize the text.
 *  - White spaces and punctuations should be removed from the result tokens.
 *  - All tokens should be converted to lower case.
 *  - Stop words should be filtered out. Use the stop word list provided in `StopWords.java`
 *
 */
public class PunctuationTokenizer implements Tokenizer {

    public static Set<String> punctuations = new HashSet<>();
    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public PunctuationTokenizer() {}

    public List<String> tokenize(String text) {
        if (text == null || text.length() == 0 || text.trim().length() == 0) return new ArrayList<>();
        char[] txtArr = text.toCharArray();
        for (int i = 0; i < txtArr.length; i++) {
            txtArr[i] = punctuations.contains(String.valueOf(txtArr[i])) ? ' ' : txtArr[i];
        }
        String newText = new String(txtArr);
        if (newText.trim().length() == 0) return Arrays.asList();
        return new ArrayList<>(Arrays.asList(newText.toLowerCase().trim().split("\\s+")).stream().filter(w -> !StopWords.stopWords.contains(w)).collect(Collectors.toList()));
    }

}
