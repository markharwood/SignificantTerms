package com.elasticsearch.facet.significantterms;

import java.io.IOException;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetExecutor.Mode;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.internal.SearchContext;

public class SignificantTermsFacetParser extends AbstractComponent implements FacetParser {

    @Inject
    public SignificantTermsFacetParser(Settings settings) {
        super(settings);
    }

    @Override
    public String[] types() {
        return new String[] { SignificantTermsFacet.TYPE };
    }

    @Override
    public Mode defaultMainMode() {
        // TODO what is the alternative "post" mode?
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public Mode defaultGlobalMode() {
        // TODO what is the alternative "post" mode?
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String currentFieldName = null;
        SignificantTermAnalysisSettings settings = new SignificantTermAnalysisSettings();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SignificantTermAnalysisSettings.ANALYZE_FIELD_NAME_TAG.equals(currentFieldName)) { 
                    settings.analyzeField = parser.text();
                }
                if (SignificantTermAnalysisSettings.DUP_LENGTH.equals(currentFieldName)) {
                    settings.duplicateParagraphWordLength = Integer.parseInt(parser.text());
                }
                if ("maxNumDocsToAnalyzePerShard".equals(currentFieldName)) {
                    settings.maxNumDocsToAnalyzePerShard = Integer.parseInt(parser.text());
                }
                if ("minGlobalDfForTopTerm".equals(currentFieldName)) {
                    settings.minGlobalDfForTopTerm = Integer.parseInt(parser.text());
                }
                if ("numTopPhrasesToReturn".equals(currentFieldName)) {
                    settings.numTopPhrasesToReturn = Integer.parseInt(parser.text());
                }
                if ("numTopTermsToReturn".equals(currentFieldName)) {
                    settings.numTopTermsToReturn = Integer.parseInt(parser.text());
                }
                if ("shardBackgroundInsufficientDocsThreshold".equals(currentFieldName)) {
                    settings.shardBackgroundInsufficientDocsThreshold = Integer.parseInt(parser.text());
                }
                if ("duplicateParagraphWordLength".equals(currentFieldName)) {
                    settings.duplicateParagraphWordLength = Integer.parseInt(parser.text());
                }
                
            }
        }
        if (settings.analyzeField == null) {
            throw new SearchSourceBuilderException(SignificantTermAnalysisSettings.ANALYZE_FIELD_NAME_TAG+
                    " must be set on significant terms facet for facet [" + facetName + "]");
        }        
        return new SignificantTermsFacetExecutor(settings);
    }
}
