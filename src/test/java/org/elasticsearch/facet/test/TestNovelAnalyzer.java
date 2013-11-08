package org.elasticsearch.facet.test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.lucene.analysis.NovelAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

//TODO write proper tests
public class TestNovelAnalyzer {

    @Test
    public void testNovelAnalyzer() throws IOException {
        NovelAnalyzer na = new NovelAnalyzer(new WhitespaceAnalyzer(Version.LUCENE_44), 4);
        for (int i = 1; i < 4; i++) {
            TokenStream ts = na.tokenStream("text",
                    new InputStreamReader(TestNovelAnalyzer.class.getResourceAsStream("/near-dups" + i + ".txt")));
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                // System.out.println(termAtt.toString() + " %dups=" +
                // na.getTotalPercentDuplicateTerms());
                System.out.print(termAtt.toString() + " ");
            }
            System.out.println("\n==============================");
            ts.end();
            ts.close();
        }
        String[] keywords = { "career", "technical", "skills", "opportunity", "like" };
        HashSet<String> keywordsSet = new HashSet<String>(Arrays.asList(keywords));
        Map<Stack<String>, Integer> runPopularities = na.getRuns(keywordsSet);
        // Print the results
        for (Entry<Stack<String>, Integer> run : runPopularities.entrySet()) {
            Stack<String> chain = run.getKey();
            for (int i = chain.size() - 1; i >= 0; i--) {
                System.out.print(chain.get(i));
                System.out.print("->");
            }
            System.out.println(run.getValue());
            System.out.println();
        }
        na.close();

    }

}
