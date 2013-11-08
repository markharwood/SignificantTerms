package com.elasticsearch.facet.significantterms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NovelAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.fieldvisitor.UidAndSourceFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

public class SignificantTermsFacetExecutor extends FacetExecutor {
    private SignificantTermAnalysisSettings settings;
    public Collection<TermStats> insufficientLocalEvidenceTerms;
    public Collection<TermStats> confirmedShardLocalTerms;
    long sampleNumDocs = 0;
    long shardNumDocs = 0;
    public Collection<PhraseStats> topPhrases;

    public SignificantTermsFacetExecutor(SignificantTermAnalysisSettings settings) {
        this.settings = settings;
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new SignificantTermsInternalFacet(facetName, insufficientLocalEvidenceTerms, confirmedShardLocalTerms, sampleNumDocs,
                shardNumDocs, topPhrases,settings);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    final class Collector extends FacetExecutor.Collector {
        private TopScoreDocCollector tdc;

        public Collector() {
            tdc = TopScoreDocCollector.create(settings.maxNumDocsToAnalyzePerShard, true);
        }

        @Override
        public void postCollection() {
            boolean sourceRequested = false;
            SignificantTermStats shardLocalConfirmedTopTerms = new SignificantTermStats(settings.numTopTermsToReturn);
            // Faster to use HashMap while gathering tokens then sort later
            Map<String, TermStats> tokens = new HashMap<String, TermStats>();
            // Map<String, TermStats> tokens = new TreeMap<String, TermStats>();
            TopDocs topDocs = tdc.topDocs();
            ScoreDoc[] sd = topDocs.scoreDocs;
            String fieldName = SignificantTermsFacetExecutor.this.settings.analyzeField;
            SearchContext context = SearchContext.current();
            AnalysisService analysisService = context.analysisService();
            Analyzer analyzer = analysisService.analyzer(fieldName);
            if (analyzer == null) {
                analyzer = analysisService.defaultIndexAnalyzer();
            }
            if (settings.duplicateParagraphWordLength > 0) {
                analyzer = new NovelAnalyzer(analyzer, settings.duplicateParagraphWordLength);
            }
            // See FetchPhase.execute
            // MapperService ms = context.mapperService();
            // Use the above for understanding document mapping
            ContextIndexSearcher searcher = context.searcher();
            int shardNumDocs = searcher.getIndexReader().numDocs();
            // NOTE: This code mostly taken from FetchPhase.java
            Set<String> fieldNames = null;
            List<String> extractFieldNames = null;
            FieldMappers x = context.smartNameFieldMappers(fieldName);
            if ((x != null) && x.mapper().fieldType().stored()) {
                if (fieldNames == null) {
                    fieldNames = new HashSet<String>();
                }
                fieldNames.add(x.mapper().names().indexName());
            } else {
                if (extractFieldNames == null) {
                    extractFieldNames = new ArrayList<String>();
                }
                extractFieldNames.add(fieldName);
            }
            FieldsVisitor fieldsVisitor = null;
            if (fieldNames != null) {
                boolean loadSource = extractFieldNames != null;
                fieldsVisitor = new CustomFieldsVisitor(fieldNames, loadSource);
            } else if (extractFieldNames != null) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
                sourceRequested = true;
            }
            Set<String> thisDocsUniqueTokens = new HashSet<String>();
            for (int i = 0; i < sd.length; i++) {
                int docId = sd[i].doc;
                thisDocsUniqueTokens.clear();
                loadStoredFields(context, fieldsVisitor, docId);
                fieldsVisitor.postProcess(context.mapperService());
                String value = null;
                // Either the field is specially held as a stored field or is
                // squirrelled away in a JSON structure as part of a "source"
                // field
                if (sourceRequested) {
                    Map<String, Object> map = SourceLookup.sourceAsMap(fieldsVisitor.source());
                    if (map != null) {
                        Object val = map.get(fieldName);
                        if (val != null) {
                            value = val.toString();
                        }
                    }
                } else {
                    if (fieldsVisitor.fields() != null) {
                        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                            // searchFields.put(entry.getKey(), new
                            // InternalSearchHitField(entry.getKey(),
                            // entry.getValue()));
                            StringBuilder sb = new StringBuilder();
                            for (Object val : entry.getValue()) {
                                sb.append(val);
                                sb.append(" ");
                            }
                            value = sb.toString();
                        }
                    }
                }
                if (value != null) {
                    TokenStream stream = null;
                    try {
                        stream = analyzer.tokenStream(fieldName, new FastStringReader(value));
                        stream.reset();
                        CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                        while (stream.incrementToken()) {
                            String t = term.toString();
                            thisDocsUniqueTokens.add(t);
                        }
                        stream.end();
                    } catch (IOException e) {
                        throw new ElasticSearchException("failed to analyze", e);
                    } finally {
                        if (stream != null) {
                            try {
                                stream.close();
                            } catch (IOException e) {
                                // ignore
                            }
                        }
                    }
                }
                for (String t : thisDocsUniqueTokens) {
                    TermStats stats = tokens.get(t);
                    if (stats == null) {
                        stats = new TermStats();
                        stats.term = t;
                        tokens.put(t, stats);
                    }
                    stats.sampleDf++;
                }
            }
            // ==================== Analyze collected terms
            // ==========================================================
            // Sort tokens for faster lookup against corpus
            Map<String, TermStats> sortedTokens = new TreeMap<String, TermStats>(tokens);
            List<TermStats> insufficientLocalEvidenceTerms = new ArrayList<TermStats>();
            List<TermStats> confirmedShardLocalTerms = new ArrayList<TermStats>();
            IndexReader reader = searcher.getIndexReader();
            for (TermStats termStat : sortedTokens.values()) {
                try {
                    termStat.shardDf = reader.docFreq(new Term(fieldName, termStat.term));
                } catch (IOException e) {
                    throw new RuntimeException("Error reading doc frequency of term " + termStat.term);
                }
                if (termStat.shardDf > 0) {
                    if ((termStat.sampleDf <= settings.minGlobalDfForTopTerm)
                            && (termStat.shardDf <= settings.shardBackgroundInsufficientDocsThreshold)) {
                        // insufficient shard-local evidence - take back to broker for
                        // global consideration
                        // TODO consider putting an upper limit of number of
                        // terms here? Could use a PriorityQueue
                        insufficientLocalEvidenceTerms.add(termStat);
                    } else {
                        // We have enough local evidence on this shard to make a
                        // yes/no decision on the importance of this term
                        termStat.score = TermStats.getSampledTermSignificance(termStat.sampleDf, sd.length, termStat.shardDf, shardNumDocs);
                        shardLocalConfirmedTopTerms.insertWithOverflow(termStat);
                    }
                } else {
                    System.err.println("Analysis process gave term not in index:" + termStat.term);
                }
            }
            while (shardLocalConfirmedTopTerms.size() > 0) {
                confirmedShardLocalTerms.add(shardLocalConfirmedTopTerms.pop());
            }
            SignificantPhraseStats topPhrases = new SignificantPhraseStats(settings.numTopPhrasesToReturn);
            // ==========Analyze top term usage in phrases ======================
            Collection<PhraseStats> returnedTopPhrases = new ArrayList<PhraseStats>();
            synchronized (SignificantTermsFacetExecutor.class) {
                if (analyzer instanceof NovelAnalyzer) {
                    NovelAnalyzer na = (NovelAnalyzer) analyzer;
                    Set<String> keywordsSet = new HashSet<String>();
                    for (TermStats ts : insufficientLocalEvidenceTerms) {
                        keywordsSet.add(ts.term);
                    }
                    for (TermStats ts : confirmedShardLocalTerms) {
                        keywordsSet.add(ts.term);
                    }
                    Map<Stack<String>, Integer> runPopularities = na.getRuns(keywordsSet);
                    for (Entry<Stack<String>, Integer> run : runPopularities.entrySet()) {
                        Stack<String> chain = run.getKey();
                        int popularity = run.getValue();
                        PhraseStats ps = new PhraseStats(chain, popularity);
                        topPhrases.insertWithOverflow(ps);
                    }
                }
                while (topPhrases.size() > 0) {
                    returnedTopPhrases.add(topPhrases.pop());
                }
            }
            // =========End analyze top term usage ===================
            SignificantTermsFacetExecutor.this.topPhrases = returnedTopPhrases;
            SignificantTermsFacetExecutor.this.insufficientLocalEvidenceTerms = insufficientLocalEvidenceTerms;
            SignificantTermsFacetExecutor.this.confirmedShardLocalTerms = confirmedShardLocalTerms;
            SignificantTermsFacetExecutor.this.sampleNumDocs = sd.length;
            SignificantTermsFacetExecutor.this.shardNumDocs = shardNumDocs;
            // }
        }

        private void loadStoredFields(SearchContext context, FieldsVisitor fieldVisitor, int docId) {
            fieldVisitor.reset();
            try {
                context.searcher().doc(docId, fieldVisitor);
            } catch (IOException e) {
                throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            tdc.collect(doc);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            tdc.setNextReader(context);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            tdc.setScorer(scorer);
        }
    }
}
