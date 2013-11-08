package com.elasticsearch.facet.significantterms;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Holds a run of significant terms.
 * TODO rationalize use of this object an {@link SignificantPhraseStats}
 */
public class PhraseStats {
    //TODO replace slow (cos Vector-backed) stack with a Deque
    public Stack<String> termsSequence;
    public int popularity;

    public PhraseStats(Stack<String> termsSequence, int popularity) {
        super();
        this.termsSequence = termsSequence;
        this.popularity = popularity;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = termsSequence.size() - 1; i >= 0; i--) {
            sb.append(termsSequence.get(i));
            if (i != 0) {
                sb.append("->");
            }
        }
        sb.append(" (" + popularity + ")");
        return sb.toString();
    }

    public List<String> getTermsAsOrderedList() {
        List<String> terms = new ArrayList<String>();
        for (int i = termsSequence.size() - 1; i >= 0; i--) {
            terms.add(termsSequence.get(i));
        }
        return terms;
    }
}
