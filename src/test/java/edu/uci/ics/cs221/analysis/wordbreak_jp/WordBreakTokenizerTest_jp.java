package edu.uci.ics.cs221.analysis.wordbreak_jp;

import edu.uci.ics.cs221.analysis.WordBreakTokenizer_jp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordBreakTokenizerTest_jp {

    @Test
    public void test1() {
        String text = "気が散る";
        List<String> expected = Arrays.asList("気", "散る");
        WordBreakTokenizer_jp tokenizer = new WordBreakTokenizer_jp();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test2() {
        String text = "関西国際空港";
        List<String> expected = Arrays.asList("関西", "国際", "空港");
        WordBreakTokenizer_jp tokenizer = new WordBreakTokenizer_jp();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test3() {
        String text = "革命を起こす";
        List<String> expected = Arrays.asList("革命", "起こす");
        WordBreakTokenizer_jp tokenizer = new WordBreakTokenizer_jp();

        assertEquals(expected, tokenizer.tokenize(text));
    }

}