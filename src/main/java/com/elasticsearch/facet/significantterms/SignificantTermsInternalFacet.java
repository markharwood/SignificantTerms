package com.elasticsearch.facet.significantterms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Stack;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

/**
 *
 */
public class SignificantTermsInternalFacet extends InternalFacet implements SignificantTermsFacet {
    private static final BytesReference STREAM_TYPE = new HashedBytesArray("significantterms".getBytes());

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readFilterFacet(in);
        }
    };
    private Collection<TermStats> confirmedShardLocalTerms;
    private Collection<TermStats> insufficientLocalEvidenceTerms;
    private long sampleNumDocs;
    private long shardNumDocs;
    private Collection<PhraseStats> topPhrases;
    private SignificantTermAnalysisSettings settings;

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    SignificantTermsInternalFacet(String facetName) {
        super(facetName);
    }

    public SignificantTermsInternalFacet(String facetName,Collection<TermStats> insufficientLocalEvidenceTerms, Collection<TermStats> confirmedShardLocalTerms,
            long sampleNumDocs, long shardNumDocs, Collection<PhraseStats> topPhrases,SignificantTermAnalysisSettings settings) {
        super(facetName);
        this.confirmedShardLocalTerms = confirmedShardLocalTerms;
        this.insufficientLocalEvidenceTerms = insufficientLocalEvidenceTerms;
        this.sampleNumDocs = sampleNumDocs;
        this.shardNumDocs = shardNumDocs;
        this.topPhrases = topPhrases;
        this.settings=settings;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Facet reduce(ReduceContext context)
    // 0.90.2 public Facet reduce(List<Facet> facets)
    {
        String debugTerm="gascoigne";
        List<Facet> facets = context.facets();
        Map<String, TermStats> returnTermStats = null;
        Map<Stack<String>, Integer> returnPhraseStats = null;
        Collection<TermStats> reducedTermStats = new ArrayList<TermStats>();
        for (Facet facet : facets) {
            SignificantTermsInternalFacet shardStats = (SignificantTermsInternalFacet) facet;
            if (returnPhraseStats == null) {
                returnPhraseStats = new HashMap<Stack<String>, Integer>();
                for (PhraseStats ps : shardStats.topPhrases) {
                    returnPhraseStats.put(ps.termsSequence, ps.popularity);
                }
            } else {
                // Merge Phrase stats
                for (PhraseStats ps : shardStats.topPhrases) {
                    Integer count = returnPhraseStats.get(ps.termsSequence);
                    if (count == null) {
                        returnPhraseStats.put(ps.termsSequence, ps.popularity);
                    } else {
                        returnPhraseStats.put(ps.termsSequence, count + ps.popularity);
                    }
                }
            }
            if (returnTermStats == null) {
                // First shard, so initialize with its contents
                returnTermStats = new HashMap<String, TermStats>();
                for (TermStats termStat : shardStats.confirmedShardLocalTerms) {
                    if(termStat.term.equals(debugTerm))
                    {
                        System.out.println("Debug Jam "+termStat.shardDf);
                    }
                    termStat.totalSampleNumDocs = shardStats.sampleNumDocs;
                    termStat.totalCorpusNumDocs = shardStats.shardNumDocs;
                    returnTermStats.put(termStat.term, termStat);
                }
                for (TermStats termStat : shardStats.insufficientLocalEvidenceTerms) {
                    termStat.totalSampleNumDocs = shardStats.sampleNumDocs;
                    termStat.totalCorpusNumDocs = shardStats.shardNumDocs;
                    if(termStat.term.equals(debugTerm))
                    {
                        System.out.println("Debug Jam "+termStat.shardDf);
                    }
                    returnTermStats.put(termStat.term, termStat);
                }                
            } else {
                for (TermStats termStat : shardStats.confirmedShardLocalTerms) {
                    TermStats other = returnTermStats.get(termStat.term);
                    if (other == null) {
                        termStat.totalSampleNumDocs = shardStats.sampleNumDocs;
                        termStat.totalCorpusNumDocs = shardStats.shardNumDocs;
                        if(termStat.term.equals(debugTerm))
                        {
                            System.out.println("Debug Jam "+termStat.shardDf);
                        }                        
                        returnTermStats.put(termStat.term, termStat);
                    } else {
                        if(termStat.term.equals(debugTerm))
                        {
                            System.out.println("Debug Jam "+termStat.shardDf);
                        }                        
                        
                        other.merge(termStat, shardStats.sampleNumDocs, shardStats.shardNumDocs);
                    }
                }
                for (TermStats termStat : shardStats.insufficientLocalEvidenceTerms) {
                    TermStats other = returnTermStats.get(termStat.term);
                    if (other == null) {
                        termStat.totalSampleNumDocs = shardStats.sampleNumDocs;
                        termStat.totalCorpusNumDocs = shardStats.shardNumDocs;
                        if(termStat.term.equals(debugTerm))
                        {
                            System.out.println("Debug Jam "+termStat.shardDf);
                        }                        
                        returnTermStats.put(termStat.term, termStat);
                    } else {
                        if(termStat.term.equals(debugTerm))
                        {
                            System.out.println("Debug Jam "+termStat.shardDf);
                        }                        
                        other.merge(termStat, shardStats.sampleNumDocs, shardStats.shardNumDocs);
                    }
                }
            }
        }

        // =========== consolidate similar phrase runs=====================
        consolidateWordRuns(returnPhraseStats);

        removeWeakTerms(returnTermStats, returnPhraseStats); 
        
        // =================================
        // Reduce to top terms using a priority queue....
       
        SignificantTermStats scoreReducedTopTerms = new SignificantTermStats(settings.numTopTermsToReturn);
        for (TermStats termStat : returnTermStats.values()) {
            if(termStat.term.equals(debugTerm))
            {
                System.out.println("Debug Jam "+termStat.shardDf);
            }            
            termStat.calculateFinalScore();
            if(termStat.score>0){                
                scoreReducedTopTerms.insertWithOverflow(termStat);
            }
        }
        while (scoreReducedTopTerms.size() > 0) {
            TermStats termStat = scoreReducedTopTerms.pop();
            reducedTermStats.add(termStat);
        }

        // reduce to top Phrases....
        ArrayList<PhraseStats> reducedPhraseStats = new ArrayList<PhraseStats>();
        if (returnPhraseStats != null) {
            SignificantPhraseStats topPhrases = new SignificantPhraseStats(settings.numTopPhrasesToReturn);
            for (Entry<Stack<String>, Integer> pse : returnPhraseStats.entrySet()) {
                int mergedPopularity = pse.getValue();

                if (mergedPopularity >= settings.minGlobalDfForTopTerm)// TODO need to use settings
                {
                    PhraseStats mergedTopPhraseStats = new PhraseStats(pse.getKey(), mergedPopularity);
                    topPhrases.insertWithOverflow(mergedTopPhraseStats);
                }
            }
            while (topPhrases.size() > 0) {
                reducedPhraseStats.add(topPhrases.pop());
            }
        }
        
//      return new SignificantTermsReducedFacet(getName(),reducedTermStats,reducedPhraseStats);
        //TODO create a different constructor (or class) for the final reduced facet results?
        return new SignificantTermsInternalFacet(getName(),null, reducedTermStats, 0l, 0l, reducedPhraseStats,settings);
                
    }

    // Removes terms that are either too rare or more frequently occur as part
    // of a phrase
    private void removeWeakTerms(Map<String, TermStats> termStats, Map<Stack<String>, Integer> phraseStats 
            ) {

        Set<String> blacklistedTerms = new HashSet<String>();
        for (Entry<String, TermStats> ts : termStats.entrySet()) {
            String word = ts.getKey();
            TermStats wordStats = ts.getValue();

            if (wordStats.sampleDf < settings.minGlobalDfForTopTerm) {
                blacklistedTerms.add(word);
                continue;
            }

            double subsumeIntoPhraseThreshold = (double) wordStats.sampleDf * settings.minPercentUsesAsPhrase;
            for (Entry<Stack<String>, Integer> thisPhrase : phraseStats.entrySet()) {
                int phrasePopularity = thisPhrase.getValue();
                Stack<String> phraseWords = thisPhrase.getKey();
                if (subsumeIntoPhraseThreshold <= (double) phrasePopularity) {
                    if (phraseWords.contains(word)) {
                        blacklistedTerms.add(word);
                    }
                }
            }
        }
        for (String blacklistedTerm : blacklistedTerms) {
            termStats.remove(blacklistedTerm);
        }

    }

    // shorter runs are subsumed into longer, more popular runs using the same
    // words (because they are likely more meaningful)
    //
    // TODO !!!!
    // It feels like there is a general purpose optimizer for merging objects that are known
    // to be subsets of each other based on signal strengths eg. the work I did on detecting which
    // ngrams in URLs were most subject to takedown notices. NGrams like "Ware" and "Warez" are 
    //known to have a containment relationship and different scores/frequencies.The score is a measure
    // of relevance (e.g. % of uses in websites names subject to takedown vs no takedowns) and frequency 
    //is the number of hits (e.g. number of relevant sites).
    //An optimization algo merges sets of candidate entities (phrases or ngrams) based on user 
    //preferences for precision vs recall and tunes into the entities that are the sweet spot. 
    // Larger entities e.g. long phrases are generally more precise but reduce recall as they are much
    // more specific. This collapsing/optimization process feels like a possibly generic algorithm capable
    // of operating on different entity types that exhibit containment hierarchies e.g geohashes, ngrams, 
    // phrases, saved query hierarchies etc).
    private void consolidateWordRuns(Map<Stack<String>, Integer> returnPhraseStats) {
        Set<Stack<String>> subsumedRuns = new HashSet<Stack<String>>();
        for (Entry<Stack<String>, Integer> thisPhrase : returnPhraseStats.entrySet()) {
            int thisPopularity = thisPhrase.getValue();
            double subsumeIntoLargerPhraseThreshold = (double) thisPopularity * settings.minPercentUsesInLargerPhrase;
            Stack<String> thisWordRun = thisPhrase.getKey();
            if (subsumedRuns.contains(thisWordRun)) {
                continue;
            }
            for (Entry<Stack<String>, Integer> otherPhrase : returnPhraseStats.entrySet()) {
                if (thisPhrase == otherPhrase) {
                    continue;
                }
                int otherPopularity = otherPhrase.getValue();
                Stack<String> otherWordRun = otherPhrase.getKey();
                if (subsumedRuns.contains(otherWordRun)) {
                    continue;
                }
                if (subsumeIntoLargerPhraseThreshold >= otherPopularity) 
                {
                    // this phrase is more popular than the other one
                    
                    if (thisWordRun.size() > otherWordRun.size()) 
                    {
                        // this phrase also has more words than the other one
                        if (thisWordRun.containsAll(otherWordRun)) {
                            // All the words in the shorter run are embodied in
                            // the longer one (e.g. "united states of america"
                            // vs "states of america")
                            subsumedRuns.add(otherWordRun);
                        }
                    }
                }

            }
        }
        for (Stack<String> wordRun : subsumedRuns) {
            returnPhraseStats.remove(wordRun);
        }
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString TERMS = new XContentBuilderString("terms");
        static final XContentBuilderString PHRASES = new XContentBuilderString("phrases");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, SignificantTermsFacet.TYPE);
        ArrayList<Map<String, Object>> topTermsForJson = new ArrayList<Map<String, Object>>();
        for (TermStats ts : confirmedShardLocalTerms) {
            Map<String, Object> termAsJsonMap = new HashMap<String, Object>();
            topTermsForJson.add(termAsJsonMap);
            termAsJsonMap.put("sampleDf", ts.sampleDf);
            termAsJsonMap.put("corpusDf", ts.shardDf);
            termAsJsonMap.put("term", ts.term);
            termAsJsonMap.put("significance", ts.score);
        }
        builder.array(Fields.TERMS, topTermsForJson.toArray());
        // =========================================================
        // ArrayList<String> topTerms = new ArrayList<String>();
        // for (TermStats ts : confirmedShardLocalTerms)
        // {
        // topTerms.add(ts.term);
        // }
        // builder.array(Fields.TERMS, topTerms.toArray(new
        // String[topTerms.size()]));
        ArrayList<Map<String, Object>> topPhrasesForJson = new ArrayList<Map<String, Object>>();
        for (PhraseStats ts : topPhrases) {
            Map<String, Object> phraseAsJsonMap = new HashMap<String, Object>();
            topPhrasesForJson.add(phraseAsJsonMap);
            phraseAsJsonMap.put("popularity", ts.popularity);
            phraseAsJsonMap.put("terms", ts.getTermsAsOrderedList());
        }
        builder.array(Fields.PHRASES, topPhrasesForJson.toArray());
        // }
        builder.endObject();
        return builder;
    }

    public static SignificantTermsFacet readFilterFacet(StreamInput in) throws IOException {
        //TODO need to chase down why this is needed - was a cut/paste/adapt from 3rd party facet
        SignificantTermsInternalFacet result = new SignificantTermsInternalFacet("Why is this required?");
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        insufficientLocalEvidenceTerms = readTermsList(in);
        confirmedShardLocalTerms = readTermsList(in);
        sampleNumDocs = in.readLong();
        shardNumDocs = in.readLong();
        topPhrases = readPhraseList(in);
    }

    public void writeTermsList(StreamOutput out, Collection<TermStats> c) throws IOException {
        if (c == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(c.size());
            for (TermStats ts : c) {
                out.writeString(ts.term);
                out.writeVInt(ts.sampleDf);
                out.writeVInt(ts.shardDf);
                out.writeDouble(ts.score);
            }
        }
    }

    static Collection<TermStats> readTermsList(StreamInput in) throws IOException {
        int count = in.readVInt();
        if (count == 0) {
            return null;
        }
        ArrayList<TermStats> result = new ArrayList<TermStats>();
        for (int i = 0; i < count; i++) {
            TermStats ts = new TermStats();
            ts.term = in.readString();
            ts.sampleDf = in.readVInt();
            ts.shardDf = in.readVInt();
            ts.score = in.readDouble();
            result.add(ts);
        }
        return result;
    }

    public void writePhraseList(StreamOutput out, Collection<PhraseStats> c) throws IOException {
        if (c == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(c.size());
            for (PhraseStats ps : c) {
                out.writeVInt(ps.popularity);
                out.writeVInt(ps.termsSequence.size());
                for (String term : ps.termsSequence) {
                    out.writeString(term);
                }
            }
        }
    }

    public Collection<PhraseStats> readPhraseList(StreamInput in) throws IOException {
        int count = in.readVInt();
        if (count == 0) {
            return null;
        }
        ArrayList<PhraseStats> result = new ArrayList<PhraseStats>();
        for (int i = 0; i < count; i++) {
            int phrasePopularity = in.readVInt();
            int numTerms = in.readVInt();
            Stack<String> chain = new Stack<String>();
            for (int j = 0; j < numTerms; j++) {
                chain.add(in.readString());
            }
            result.add(new PhraseStats(chain, phrasePopularity));
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        {
            writeTermsList(out, insufficientLocalEvidenceTerms);
            writeTermsList(out, confirmedShardLocalTerms);
            out.writeLong(sampleNumDocs);
            out.writeLong(shardNumDocs);
            writePhraseList(out, topPhrases);
        }
    }

    @Override
    public Collection<TermStats> getTopTerms() {
        return confirmedShardLocalTerms;
    }

    @Override
    public Collection<PhraseStats> getTopPhrases() {
        return topPhrases;
    }
}
