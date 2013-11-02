package com.inperspective.topterms;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * This code is the intellectual property of Mark Harwood - the algorithms are
 * those featured in the "TopTermFinder" related classes found in
 * inperspective.jar. This is granted under a non-exclusive license to Detica
 * for redistribution in binary form and comes with absolutely no warranties.
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
