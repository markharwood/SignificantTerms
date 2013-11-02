package com.elasticsearch.facet.significantterms;

import org.apache.lucene.util.PriorityQueue;

import com.inperspective.topterms.PhraseStats;

public class SignificantPhraseStats extends PriorityQueue<PhraseStats> {
    public SignificantPhraseStats(int size) {
        super(size);
    }

    @Override
    protected boolean lessThan(PhraseStats a, PhraseStats b) {
        return a.popularity < b.popularity;
    }
}
