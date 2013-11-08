package com.elasticsearch.facet.significantterms;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

public class SignificantTermsFacetBuilder extends FacetBuilder {

    //Initialize with default settings
    SignificantTermAnalysisSettings settings=new SignificantTermAnalysisSettings();
    
    public SignificantTermsFacetBuilder(String name) {
        super(name);
    }
    
    //TODO add other builder-style setters to allow override of all properties in settings object
    
    public SignificantTermsFacetBuilder duplicateParagraphWordLength(int length)
    {
        settings.duplicateParagraphWordLength=length;
        return this;
    }
    public SignificantTermsFacetBuilder analyzeField(String analyzeField)
    {
        settings.analyzeField=analyzeField;
        return this;
    }
    public SignificantTermsFacetBuilder shardBackgroundInsufficientDocsThreshold(int threshold)
    {
        settings.shardBackgroundInsufficientDocsThreshold=threshold;
        return this;
    }
    

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (settings.analyzeField == null) {
            throw new SearchSourceBuilderException(SignificantTermAnalysisSettings.ANALYZE_FIELD_NAME_TAG+
                    " must be set on significant terms facet for facet [" + name + "]");
        }        
        builder.startObject(name);
        builder.startObject(SignificantTermsFacet.TYPE);
        if (settings.analyzeField != null) {
            builder.field(SignificantTermAnalysisSettings.ANALYZE_FIELD_NAME_TAG, settings.analyzeField);
        }  
        builder.field(SignificantTermAnalysisSettings.DUP_LENGTH,settings.duplicateParagraphWordLength);
        
        builder.endObject();

        //TODO copied from TermsFacetBuilder and I'm not sure if all of these settings apply
        // e.g. doesn't make sense to run facet without *any* query - no significance to sample?
        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }

}
