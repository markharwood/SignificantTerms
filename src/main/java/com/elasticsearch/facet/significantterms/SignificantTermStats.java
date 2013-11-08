package com.elasticsearch.facet.significantterms;

import org.apache.lucene.util.PriorityQueue;

public class SignificantTermStats extends PriorityQueue<TermStats> {
    public SignificantTermStats(int size) {
        super(size);
    }

    @Override
    protected boolean lessThan(TermStats a, TermStats b) {
        return a.score < b.score;
    }
}
