package org.elasticsearch.facet.test;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.index.query.FieldQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.facet.Facets;
import org.junit.Assert;

import com.elasticsearch.facet.significantterms.PhraseStats;
import com.elasticsearch.facet.significantterms.SignificantTermsFacet;
import com.elasticsearch.facet.significantterms.SignificantTermsFacetBuilder;
import com.elasticsearch.facet.significantterms.TermStats;


//TODO add test statements - need to finish tuning plugin defaults before
// recording expected outputs in a test
public class TestSignificantTermsFacet {

    private static final String FACT_DOC_TYPE = "fact";
    private static final String FACTS_INDEX_NAME = "facts";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Node node = nodeBuilder().local(true).node();
        node.start();
        Client client = node.client();
        
        //Setup the index content
        createIndexWithMapping(client);
        indexFacts(loadFacts(), node);
        
        //Run tests
        runQueries(client);
        
        System.out.println("Closing...");
        
        
        node.stop();

        node.close();
    }

    private static void runQueries(Client client) {

        SearchResponse response = client.prepareSearch(FACTS_INDEX_NAME)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new FieldQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addFacet(new SignificantTermsFacetBuilder("mySignificantTerms").analyzeField("Description")
                            //We wouldn't set the threshold this low outside of a low-scale test like this
                            // as it would drag lots of terms stats broker-side in a full-scale cluster
                           .shardBackgroundInsufficientDocsThreshold(2)
                           
                        )
                .execute()
                .actionGet();
        SearchHit[] results = response.getHits().getHits();
        for (SearchHit hit : results) {
//          System.out.println(hit.getId());    //prints out the id of the document
          Map<String,Object> result = hit.getSource();   //the retrieved document
          System.out.println(result.get("Description"));
        }      
        Facets facets = response.getFacets();
        SignificantTermsFacet signifFacet = facets.facet("mySignificantTerms");
        for(PhraseStats phrase:signifFacet.getTopPhrases())
        {
            System.out.println(phrase);
        }
        for(TermStats termStats:signifFacet.getTopTerms())
        {
            System.out.println(termStats);
        }

        
    }

    static void indexFacts(List<Map<String, Object>> products, Node node) throws ExecutionException, InterruptedException  {
        long currentCount = getCurrentDocumentCount(FACTS_INDEX_NAME, node);
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> product : products) {
//            IndexRequest indexRequest = new IndexRequest(index, "fact", (String) product.get("ProductId"));
            IndexRequest indexRequest = new IndexRequest(FACTS_INDEX_NAME, FACT_DOC_TYPE);
            indexRequest.source(product);
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = node.client().bulk(bulkRequest).actionGet();
        if (response.hasFailures()) {
            Assert.fail("Error in creating facts: " + response.buildFailureMessage());
        }
        refreshIndex(FACTS_INDEX_NAME, node);
        assertDocumentCountAfterIndexing(FACTS_INDEX_NAME, products.size() + currentCount, node);
    }

    public static void refreshIndex(String index, Node node) throws ExecutionException, InterruptedException {
        node.client().admin().indices().refresh(new RefreshRequest(index)).get();
    }

    public static void assertDocumentCountAfterIndexing(String index, long expectedDocumentCount, Node node) {
        assertThat(getCurrentDocumentCount(index, node), is(expectedDocumentCount));
    }

    public static long getCurrentDocumentCount(String index, Node node) {
        return node.client().prepareCount(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(2000).getCount();
    }

    // Loads CSV file full of test data into RAM
    public static List<Map<String, Object>> loadFacts() throws IOException {
        BufferedReader r = new BufferedReader(new InputStreamReader(TestSignificantTermsFacet.class.getResourceAsStream("/facts.csv")));
        String text = r.readLine();
        List<Map<String, Object>> facts = Lists.newArrayList();

        String[] headers = text.split(",");
        text = r.readLine();
        while (text != null) {
            String[] cols = text.split(",");

            Map<String, Object> fact = Maps.newHashMap();
            for (int j = 0; j < cols.length; j++) {
                fact.put(headers[j], cols[j]);
            }
            facts.add(fact);
            text = r.readLine();
        }
        return facts;
    }

    public static void createIndexWithMapping(Client client) throws IOException {
        String index = FACTS_INDEX_NAME;
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(index).execute().actionGet();
        if (existsResponse.isExists()) {
            System.out.println("Deleting existing index" + System.currentTimeMillis());
            client.admin().indices().delete(new DeleteIndexRequest(index));
        }
        Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("index.number_of_shards", 2);
        settingsBuilder.put("index.number_of_replicas", 0);
        
        System.out.println("Creating " + System.currentTimeMillis());
        String mapping = IOUtils.toString(TestSignificantTermsFacet.class.getResourceAsStream("/factmapping.json"));
        client.admin().indices().prepareCreate(index).setSettings(settingsBuilder).execute().actionGet();
        client.admin().indices().preparePutMapping(index).setType(FACT_DOC_TYPE).setSource(mapping).execute().actionGet();
    }

}
