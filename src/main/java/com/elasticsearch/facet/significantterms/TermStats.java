package com.elasticsearch.facet.significantterms;

/**
 * Holds stats about use of a term both in a sample and in a background corpus
 * @author Mark
 *
 */
public class TermStats {
    public String term;
    public int sampleDf;
    public int shardDf;
    public double score;
    public long totalSampleNumDocs;
    public long totalCorpusNumDocs;

    @Override
    public String toString() {
        return "TermStats [term=" + term + ", sampleDf=" + sampleDf + ", shardDf=" + shardDf + "]" + " score=[" + score + "]";
    }

    public void merge(TermStats other, long sampleNumDocs, long shardNumDocs) {
        totalSampleNumDocs += sampleNumDocs;
        totalCorpusNumDocs += shardNumDocs;
        sampleDf += other.sampleDf;
        shardDf += other.shardDf;
    }

 

    // Calculates the significance of a term in a sample against a background of
    // normal distributions TODO - allow pluggable scoring implementations
    public static final double getSampledTermSignificance(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        double subsetProbability = (double) subsetFreq / (double) subsetSize;
        double supersetProbability = (double) supersetFreq / (double) supersetSize;
        
        // uplift score favours very commons words e.g. you, we etc
        double uplift = subsetProbability - supersetProbability; 
        if (uplift <= 0) {
            return 0;
        }
        // damerau score tends to favour rarer terms e.g.mis-spellings
        double damerauScore = (subsetProbability / supersetProbability); 
        // A blend of the above - favours medium-rare terms        
        double score = (uplift * uplift) * damerauScore; 
        return score;
    }

    //calculated once all of the stats from various shards assembled
    public void calculateFinalScore() {
        score = getSampledTermSignificance(sampleDf, totalSampleNumDocs, shardDf, totalCorpusNumDocs);
    }
}
