package com.elasticsearch.facet.significantterms;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

public class SignificantTermsPlugin extends AbstractPlugin {
    public String name() {
        return "significantTerms";
    }

    public String description() {
        return "Fetches the significant terms for a selection of top-scoring documents";
    }

    public void onModule(FacetModule module) {
        module.addFacetProcessor(SignificantTermsFacetParser.class);
    }
}
