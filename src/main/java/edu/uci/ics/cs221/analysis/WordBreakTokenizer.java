package edu.uci.ics.cs221.analysis;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 *
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 *
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 *
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 *
 * Requirements:
 *  - Use Dynamic Programming for efficiency purposes.
 *  - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 *      The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 *  - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 *  - Stop words should be removed.
 *  - If there's no possible way to break the string, throw an exception.
 *
 */
public class WordBreakTokenizer implements Tokenizer {

    // key: string of the word, value:
    private Map<String, Double> wordDict;

    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
            wordDict = new HashMap<>();
            for (String dict : dictLines) {
                String[] dictInfo = dict.split(" ");
                if (dictInfo[0].startsWith("\uFEFF")) {
                    dictInfo[0] = dictInfo[0].substring(1);
                }
                // 2147483647 is the overall frequency of all words
                wordDict.put(dictInfo[0], Math.log(Double.valueOf(dictInfo[1]) / 5.41613984156E11));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {
        if (text == null || text.length() == 0) {
            return new ArrayList<>();
        }
        int len = text.length();
        text = text.toLowerCase();

        // initiate a dynamic programming matrix
        // recording whether the corresponding sliding window can be split or not
        boolean[][] exist = new boolean[len][len];

        // initiate a probability matrix
        // recording the maximum probability of the corresponding sliding window
        double[][] probability = new double[len][len];

        // initiate a split point matrix
        // recording the optimal split position to gain the best probability
        // split[start][end] = start means the string is not split in the optimal case
        int[][] split = new int[len][len];

        // fill in the dynamic programming matrix
        for (int strLen = 1; strLen <= len; strLen++) {
            for (int start = 0; start <= len - strLen; start++) {
                int end = start + strLen - 1;
                double maxProbability = -Double.MAX_VALUE;

                // choose the split position to check out whether there is a combination with higher probability
                // pos means make a cut before this index
                // at this stage, the length of the string will be no smaller than 2
                for (int pos = start; pos < end + 1; pos++) {
                    String secondHalf = text.substring(pos, end + 1);
                    // check the whole string
                    if (start == pos) {
                        if (wordDict.containsKey(secondHalf)) {
                            exist[start][end] = true;
                            maxProbability = wordDict.get(secondHalf);
                            split[start][end] = start;
                            // if it is already a existing word, we just break
                            break;
                        }
                    } else { // split to find optimal probability
                        if (exist[start][pos - 1] && exist[pos][end]) {
                            exist[start][end] = true;
                            if (probability[start][pos - 1] + probability[pos][end] > maxProbability) {
                                exist[start][end] = true;
                                maxProbability = probability[start][pos - 1] + probability[pos][end];
                                split[start][end] = pos;
                            }
                        }
                    }
                }
                probability[start][end] = maxProbability;
            }
        }

        // throw exception if the word can't be stemmed
        if (!exist[0][text.length() - 1]) {
            throw new RuntimeException("Given String can't be broken.");
        }

        // use recursive function find split point
        List<Integer> splitPoint = new ArrayList<>();
        findSplitPoint(split, splitPoint, 0, text.length() - 1);
        Collections.sort(splitPoint);

        List<String> result = new ArrayList<>();
        for (int i = 0; i < splitPoint.size(); i++) {
            int currentCut = splitPoint.get(i);
            if (i == 0) {
                String current = text.substring(0, currentCut);
                if (!StopWords.stopWords.contains(current) && current.length() != 0) {
                    result.add(current);
                }
            } else {
                int previousCut = splitPoint.get(i - 1);
                String current = text.substring(previousCut, currentCut);
                if (!StopWords.stopWords.contains(current)) {
                    result.add(current);
                }
            }
        }
        String trialString = text.substring(splitPoint.get(splitPoint.size() - 1));
        if (!StopWords.stopWords.contains(trialString)) {
            result.add(trialString);
        }
        return result;
    }

    private void findSplitPoint(int[][] split, List<Integer> splitPoint, int start, int end) {
        if (start == split[start][end]) {
            splitPoint.add(start);
            return;
        }
        int cutPoint = split[start][end];
        findSplitPoint(split, splitPoint, start, cutPoint - 1);
        findSplitPoint(split, splitPoint, cutPoint, end);
    }
}