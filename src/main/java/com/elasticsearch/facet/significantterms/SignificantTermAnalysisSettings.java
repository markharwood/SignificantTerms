package com.elasticsearch.facet.significantterms;


public class SignificantTermAnalysisSettings {
    public static final String ANALYZE_FIELD_NAME_TAG = "analyzeField";
    public static final String DUP_LENGTH = "duplicateParagraphWordLength";
    
    //mandatory field - need to determine which field we analyze for significant terms
    String analyzeField = null;
    int minGlobalDfForTopTerm = 2;
    //The quality measure that detects "ill-informed" shards incapable of making a local-only decision on a term.
    //The higher the number the more terms must be dragged back to the broker for global analysis. The measure 
    //is the term's background doc frequency in a shard, not the foreground search results sample
    int shardBackgroundInsufficientDocsThreshold = 5;
    public int numTopTermsToReturn = 20;
    public int numTopPhrasesToReturn=20;
    //To combat repeated text messing up our stats e.g. boiler-plate disclaimers or retweets we use
    //an analyzer that spots runs of previously seen words and removes them from consideration
    public int duplicateParagraphWordLength = 5;
    //Used to consolidate small phrases e.g. "united states" into longer e.g. "united states of america" 
    //Controlled by a number between 0 and 1 where 1 means 100% of sightings must be as part of a larger phrase
    public double minPercentUsesInLargerPhrase=0.7;
    
    //A top term is removed if at least 60% of uses as part of a phrase e.g. "united" always seen in "united states"
    public double minPercentUsesAsPhrase=0.6;
    //Need a decent number of documents as a sample for stats to do their work - this is a per-shard number
    //TODO for indexes with larger docs we could use fewer docs but fragment their text content 
    //into many sub-docs e.g. paragraphs for stats to work on. Just need reasonable volumes to
    //get percentage figures e.g. "5 out of 100 paragraphs" % in results sample could be compared with 
    //"7 out of 1m docs" % stats - it's not strictly apples vs apples but still indicative of vocab popularity  
    public int maxNumDocsToAnalyzePerShard=200;
}
