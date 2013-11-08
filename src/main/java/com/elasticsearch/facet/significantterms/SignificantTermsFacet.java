package com.elasticsearch.facet.significantterms;

import java.util.Collection;

import org.elasticsearch.search.facet.Facet;

public interface SignificantTermsFacet extends Facet {
    public static final String TYPE = "significantTerms";
    public Collection<TermStats> getTopTerms();
    public Collection<PhraseStats> getTopPhrases();
    
}
