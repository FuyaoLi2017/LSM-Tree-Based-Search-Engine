package edu.uci.ics.cs221.analysis;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Project 1, task 4: Implement a Dynamic-programming Based Japanese Tokenizer .
 *
 * Tokenizing Japanese text is challenging because there are no explicit spaces between words.
 * It is very similar to the word-break problem in task 2.
 *
 * Use the same dictionary-frequency and dynamic programming based algorithm in task 2 to implement a Japanese Tokenizer.
 * For fairness, you must choose a language that is NOT your native language.
 *
 * You need to find a Japanese dictionary corpus with frequency information on your own, and write at least 3 test cases to test the correctness of your tokenizer.
 *
 */
public class WordBreakTokenizer_jp implements Tokenizer {

    // key: string of the word, value:
    private Map<String, Double> wordDict;

    public WordBreakTokenizer_jp() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer_jp.class.getClassLoader().getResource("cs221_frequency_dictionary_ja.txt");

            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
            wordDict = new HashMap<>();
            for (String dict : dictLines) {
                String[] dictInfo = dict.split(" ");
                if (dictInfo[0].startsWith("\uFEFF")) {
                    dictInfo[0] = dictInfo[0].substring(1);
                }
                wordDict.put(dictInfo[0], Double.valueOf(dictInfo[1]) / 663428); // divided by frequency sum up
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

                double maxProbability = 0;

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
                        }
                    } else { // split to find optimal probability
                        if (exist[start][pos - 1] && exist[pos][end]) {
                            exist[start][end] = true;
                            if (probability[start][pos - 1] * probability[pos][end] > maxProbability) {
                                exist[start][end] = true;
                                maxProbability = probability[start][pos - 1] * probability[pos][end];
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
            throw new RuntimeException();
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
                if (!StopWords_jp.stopWords.contains(current) && current.length() != 0) {
                    result.add(current);
                }
            } else {
                int previousCut = splitPoint.get(i - 1);
                String current = text.substring(previousCut, currentCut);
                if (!StopWords_jp.stopWords.contains(current)) {
                    result.add(current);
                }
            }
        }
        String trialString = text.substring(splitPoint.get(splitPoint.size() - 1));
        if (!StopWords_jp.stopWords.contains(trialString)) {
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
