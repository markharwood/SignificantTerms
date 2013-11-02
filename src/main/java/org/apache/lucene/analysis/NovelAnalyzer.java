package org.apache.lucene.analysis;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeSource.State;

/**
 * An Analyzer which only emits tokens for "novel" text ie sequences of words not seen before. This
 * has uses in:
 * <ol>
 * <li>Stripping "boiler plate" text e.g copyright notices</li>
 * <li>Stripping repeated text e.g copied sections of emails in a thread</li>
 * </ol>
 * The class prunes the information kept from multiple documents using a sliding window of content
 * which is used to search for duplicate text.
 * Post-analysis it is also possible to extract stats on the use of selected keywords in phrases
 * e.g. to find the relevant phrases for the top terms ["grand","f1", "driver", "prix", "racing"]
 * @author MAHarwood
 */
public final class NovelAnalyzer extends Analyzer
{
    private Analyzer delegate;
    //TODO look at CharArrayMap (?) etc in core Lucene to avoid instantiating Strings for terms?
    HashMap<String, List<ChainedToken>> tokens = new HashMap<String, List<ChainedToken>>();
    int totalNumInputTokens = 0;
    int totalNumDupTokensRemoved = 0;
    int numInputTokensInLastTokenStream;
    int numDuplicateTokensRemovedFromLastTokenStream;
    int numTokenStreamsConsidered;
    int minDupChainLength = 10;
    int lastPruneDocId = 0;
    int maxNumCachedDocs = 300;
    HashSet<String> stopWords = new HashSet<String>();

    public NovelAnalyzer(Analyzer analyzer, int minDupChainLength)
    {
        this.delegate = analyzer;
        this.minDupChainLength = minDupChainLength;
    }

    public Map<Stack<String>, Integer> getRuns(Set<String> keywords)
    {
        Map<Stack<String>, Set<Integer>> runDocIds = new HashMap<Stack<String>, Set<Integer>>();
        for (String keyword : keywords)
        {
            List<ChainedToken> prevOccs = tokens.get(keyword);
            if (prevOccs != null)
            {
                Stack<String> chain = new Stack<String>();
                //For all prior sighting of this keyword..
                for (ChainedToken tokenOcc : prevOccs)
                {
                    ChainedToken currToken = tokenOcc;
                    chain.clear();
                    int chainContainedInDocId=0;
                    while (keywords.contains(currToken.currToken))
                    {
                    	chainContainedInDocId=currToken.srcDocId;
                    	//I did experiment with allowing up to n words in  a run which weren't explicitly listed in "keywords"
                    	// in order to help include useful joining words (eg "of", "the" "and") alongside the statistically 
                    	//interesting keywords you might pass as keywords to this function. 
                    	// The quality of results wasn't too great in my tests so I would stick with the strict inclusion of only
                    	// words in the keywords set into runs. If a client needs boring-but-useful connecting words in the runs
                    	// they should be added to the keywords list (but this may match runs like "and the" so maybe there is 2 tier
                    	// system required where there is a set of mandatory keywords and a set of optional ones- the rule being a 
                    	// run must have at least one mandatory keyword in a run.
                        chain.add(currToken.currToken);
                        if (currToken.prevToken == null)
                        {
                            break;
                        }
                        //Keep chasing backwards..
                        currToken = currToken.prevToken;
                    }
                    if (chain.size() > 1)
                    {
                        Stack<String> savedChain = (Stack<String>) chain.clone();
                        Set<Integer> uniqueDocIdsWithThisChain = runDocIds.get(savedChain);
                        if(uniqueDocIdsWithThisChain==null)
                        {
                        	//TODO a hashset maybe a little heavyweight here  - we know num of docs parsed and only need a 1/0 bit to mark presence of run in doc?
                        	uniqueDocIdsWithThisChain=new HashSet<Integer>();
                        	runDocIds.put(savedChain,uniqueDocIdsWithThisChain);
                        }
                        uniqueDocIdsWithThisChain.add(chainContainedInDocId);
                    }
                }
            }
        }
        //Count the number of docs
        Map<Stack<String>, Integer> runPopularities = new HashMap<Stack<String>, Integer>();
        for (Entry<Stack<String>, Set<Integer>> chainStats : runDocIds.entrySet()) {
        	runPopularities.put(chainStats.getKey(), chainStats.getValue().size());
		}
      
        return runPopularities;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader)
    {
        numInputTokensInLastTokenStream = 0;
        numDuplicateTokensRemovedFromLastTokenStream = 0;
        if ((numTokenStreamsConsidered - lastPruneDocId) > maxNumCachedDocs)
        {
            prune();
        }
        numTokenStreamsConsidered++;
        TokenStreamComponents srcComp = delegate.createComponents(fieldName, reader);
        Tokenizer source = srcComp.source;
        TokenStream filter = new NovelTokenFilter(srcComp.sink);
        return new TokenStreamComponents(source, filter);
    }

    private void prune()
    {
        //prune half of the docs in our window so we keep at least some in tree
        int pruneUpTo = numTokenStreamsConsidered - (maxNumCachedDocs / 2);
        //      System.out.println("Pruning to "+pruneUpTo);
        lastPruneDocId = numTokenStreamsConsidered;
        HashSet<String> redundantTokens = new HashSet<String>();
        int occsPruned = 0;
        int termsPruned = 0;
        int termsFastPruned = 0;
        //      long oPruneStart=System.currentTimeMillis();
        for (List<ChainedToken> prevOccs : tokens.values())
        {
            String currToken = null;
            //fast exit - if last occurence is outside scope bin all occurences and token from hashmap 
            ChainedToken lastOcc = prevOccs.get(prevOccs.size() - 1);
            currToken = lastOcc.currToken;
            //flawed logic - prevOccs.size is total term TFs NOT a term DF so 
            //"percent" is not actually a value between 0 and 1
            float percentOfDocsInSet = ((float) prevOccs.size()) / (float) maxNumCachedDocs;
            //See above comment. I have found values >=10 for percentOfDocsInSet identifies 
            // a useful set of stop words - may need to expose a property to control this
            float stopWordThresholdFactor = 10f;
            if (
            //prune words which will have no occurences
            (lastOcc.srcDocId < pruneUpTo) ||
            //prune stop words
                    (percentOfDocsInSet > stopWordThresholdFactor))
            {
                if (percentOfDocsInSet > stopWordThresholdFactor)
                {
                    stopWords.add(currToken);
                    System.out.println("Stop-wording " + currToken.toString() + " "
                            + percentOfDocsInSet);
                }
                redundantTokens.add(currToken);
                occsPruned += prevOccs.size();
                termsPruned++;
                termsFastPruned++;
                continue; //fast exit - all occs predate the prune position
            }
            HashSet<ChainedToken> redundantOccs = new HashSet<ChainedToken>();
            for (ChainedToken chainedToken : prevOccs)
            {
                if (chainedToken.srcDocId < pruneUpTo)
                {
                    redundantOccs.add(chainedToken);
                    occsPruned++;
                }
                else
                {
                    break;
                }
            }
            prevOccs.removeAll(redundantOccs);
        }
        for (String redundantToken : redundantTokens)
        {
            tokens.remove(redundantToken);
        }
    }

    public float getTotalPercentDuplicateTerms()
    {
        return (float) totalNumDupTokensRemoved / (float) totalNumInputTokens;
    }

    public float getPercentDuplicateTermsInLastTokenStream()
    {
        return (float) numDuplicateTokensRemovedFromLastTokenStream
                / (float) numInputTokensInLastTokenStream;
    }

    class NovelTokenFilter extends TokenFilter
    {
        //        private TokenStream delegateTokenStream;
        private ChainedToken lastChainedToken;
        private Stack<State> validTokens;
        int docId;
        CharTermAttribute termAtt;

        public NovelTokenFilter(TokenStream in)
        {
            super(in);
            termAtt = addAttribute(CharTermAttribute.class);
        }

        @Override
        public boolean incrementToken() throws IOException
        {
            if (validTokens == null)
            {
                loadStack();
            }
            clearAttributes();
            if (validTokens.size() > 0)
            {
                State earlierToken = validTokens.pop();
                restoreState(earlierToken);
                return true;
            }
            else
            {
                return false;
            }
        }

        public void loadStack() throws IOException
        {
            int checkEvery = Math.max(1, minDupChainLength - 5);
            validTokens = new Stack<State>();
            ChainedToken currToken = null;
            ChainedToken fastTrackToken = null;
            while (input.incrementToken())
            {
                totalNumInputTokens++;
                numInputTokensInLastTokenStream++;
                String currTermText = termAtt.toString();
                currToken = new ChainedToken(docId, lastChainedToken, captureState(), currTermText);
                if (fastTrackToken != null)
                {
                    //we have a confirmed match (>minDupChainLength tokens in a row) and now focus
                    //exclusively on following this chain through until the end                 
                    if ((fastTrackToken.nextToken != null)
                            && (currTermText.equals(fastTrackToken.nextToken.currToken)))
                    {
                        fastTrackToken = fastTrackToken.nextToken;
                        currToken.maxChainLength = currToken.prevToken.maxChainLength + 1;
                        //                        System.out.println(fastTrackToken.getChainedText());
                    }
                    else
                    {
                        //trail has gone cold .. consider all alternatives here on in
                        fastTrackToken = null;
                    }
                }
                if (!stopWords.contains(currTermText))
                {
                    List<ChainedToken> prevOccs = tokens.get(currTermText);
                    if (prevOccs != null)
                    {
                        if (fastTrackToken == null)
                        {
                            int thisTokensChainLength = 0;
                            // only examine chains every now and then - avoids excessive comparison by looking back on every single word
                            if ((totalNumInputTokens % checkEvery) == 0)
                            {
                                //we have advanced sufficiently to now spend time taking stock of any overlaps
                                for (ChainedToken otherChainedToken : prevOccs)
                                {
                                    ChainedToken endOtherChainedToken = otherChainedToken;
                                    ChainedToken thisChainedToken = currToken;
                                    int sameChainLength = 0;
                                    while (thisChainedToken.currToken
                                            .equals(otherChainedToken.currToken))
                                    {
                                        sameChainLength++;
                                        if (thisChainedToken.prevToken == null)
                                        {
                                            break;
                                        }
                                        if (otherChainedToken.prevToken == null)
                                        {
                                            break;
                                        }
                                        otherChainedToken = otherChainedToken.prevToken;
                                        thisChainedToken = thisChainedToken.prevToken;
                                    }
                                    if (sameChainLength > thisTokensChainLength)
                                    {
                                        thisTokensChainLength = sameChainLength;
                                        //if we have >5 shared tokens in a row we are onto something - focus exclusively
                                        //on testing this chain going forward rather than current approach of 
                                        // exhaustively backward testing all chains that lead up to this token 
                                        if (thisTokensChainLength > 5)
                                        {
                                            fastTrackToken = endOtherChainedToken;
                                        }
                                    }
                                }
                            }
                            currToken.maxChainLength = thisTokensChainLength;
                        }
                    }
                    else
                    //a token we've not seen  before
                    {
                        prevOccs = new ArrayList<ChainedToken>();
                        tokens.put(currTermText, prevOccs);
                    }
                    prevOccs.add(currToken);
                }
                //tie up lastChainedToken to point to this one
                if (lastChainedToken != null)
                {
                    lastChainedToken.nextToken = currToken;
                }
                //replace last Token with this one.
                lastChainedToken = currToken;
            }//loop round on delegateTokenStream.incrementToken
            ChainedToken cursor = currToken;
            int ignoreCount = 0;
            while (cursor != null)
            {
                if (ignoreCount <= 0)
                {
                    if (cursor.maxChainLength > minDupChainLength)
                    {
                        ignoreCount = cursor.maxChainLength;
                    }
                    else
                    {
                        if (cursor.currTokenState == null)
                        {
                            throw new IllegalStateException("Error - token is already emitted");
                        }
                        validTokens.push(cursor.currTokenState);
                        cursor.currTokenState = null; //remove state for emitted novel token
                    }
                }
                else
                {
                    cursor.currTokenState = null; //remove the state of this duplicate token
                    ignoreCount--;
                    totalNumDupTokensRemoved++;
                    numDuplicateTokensRemovedFromLastTokenStream++;
                }
                cursor = cursor.prevToken;
            }
        }

        @Override
        public final void reset() throws IOException
        {
            super.reset();
            docId++;
            if (validTokens != null)
            {
                validTokens.clear();
                validTokens = null;
            }
            lastChainedToken = null;
        }
    }

    static class ChainedToken
    {
        //Count of duplicate tokens up to this point 
        public int maxChainLength;
        ChainedToken prevToken;
        String currToken;
        ChainedToken nextToken;
        int count;
        int srcDocId;
        State currTokenState;

        public ChainedToken(int srcDocId, ChainedToken prevToken, State currTokenState,
                String currToken)
        {
            this.srcDocId = srcDocId;
            this.prevToken = prevToken;
            this.currToken = currToken;
            this.currTokenState = currTokenState;
        }

        public String getChainedText()
        {
            String[] prevTokens = new String[maxChainLength];
            ChainedToken cursor = this;
            for (int i = maxChainLength - 1; i >= 0; i--)
            {
                prevTokens[i] = cursor.currToken;
                cursor = cursor.prevToken;
            }
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < prevTokens.length; i++)
            {
                result.append(prevTokens[i]);
                result.append(" ");
            }
            return result.toString();
        }
    }

    public int getMaxNumCachedDocs()
    {
        return maxNumCachedDocs;
    }

    public void setMaxNumCachedDocs(int maxNumCachedDocs)
    {
        this.maxNumCachedDocs = maxNumCachedDocs;
    }
}
