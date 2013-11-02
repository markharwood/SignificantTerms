package com.inperspective.topterms;

/**
 * This code is the intellectual property of Mark Harwood - the
 * "term significance" algorithm is the one featured in the "TopTermFinder"
 * class found in inperspective.jar. This is granted under a non-exclusive
 * license to Detica for redistribution in binary form and comes with absolutely
 * no warranties.
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
//        score = getSampledTermSignificance(sampleDf, totalSampleNumDocs, shardDf, totalCorpusNumDocs);
    }

    // Calculates the significance of a term in a sample against a background of
    // normal distributions using LingPipe algo
    // shown here:
    // http://lingpipe-blog.com/2006/03/29/interesting-phrase-extraction-binomial-hypothesis-testing-vs-coding-loss/
    public static final double lingPipegetSampledTermSignificance(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        double subsetProbability = (double) subsetFreq / (double) subsetSize;
        double supersetProbability = (double) supersetFreq / (double) supersetSize;
        double uplift = subsetProbability - supersetProbability; // favours very
                                                                 // commons
                                                                 // words e.g.
                                                                 // you, we etc
        if (uplift <= 0) {
            return 0;
        }
        // see
        // http://lingpipe-blog.com/2006/03/29/interesting-phrase-extraction-binomial-hypothesis-testing-vs-coding-loss/
        double score = (uplift * uplift) / ((1d - supersetProbability) * supersetProbability);
        return score;
    }

    // Calculates the significance of a term in a sample against a background of
    // normal distributions
    public static final double getSampledTermSignificance(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        double subsetProbability = (double) subsetFreq / (double) subsetSize;
        double supersetProbability = (double) supersetFreq / (double) supersetSize;
        double uplift = subsetProbability - supersetProbability; // favours very
                                                                 // commons
                                                                 // words e.g.
                                                                 // you, we etc
        if (uplift <= 0) {
            return 0;
        }
        double damerauScore = (subsetProbability / supersetProbability); // damerau
                                                                         // tends
                                                                         // to
                                                                         // favour
                                                                         // rarer
                                                                         // terms
                                                                         // e.g.mis-spellings
        double score = (uplift * uplift) * damerauScore; // A blend of the above
                                                         // - favours
                                                         // medium-rare terms
        return score;
    }

    //calculated once all of the stats from various shards assembled
    public void calculateFinalScore() {
        score = getSampledTermSignificance(sampleDf, totalSampleNumDocs, shardDf, totalCorpusNumDocs);
    }
}
