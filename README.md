# Significant terms plugin for Elasticsearch


## Overview

The significant terms plugin analyses search results and identifies words or phrases that are *significant* (as opposed to merely popular). 
In a search for *"bird flu"* the plugin may suggest *"H5n1"* as a significant term (or vice-versa). 
The term *H5n1* would be suggested as significant even if it is mentioned say only 3 times in the top 100 search results - if it only evers occurs 4 times in a corpus of 10 million docs it is highly likely that it is pertinent.

### Syntax
The plugin is written as a facet:

	"query" :
	{
		 "query_string" : {
		     	    "query" : "\"bird flu\""
    	}
   	},
   	"facets" : {
	        "significant":{
	                    "significantTerms":{
		                    "analyzeField":"Body",
		                    "numTopPhrasesToReturn":10,
		                    "numTopTermsToReturn":20
		                    }
		     }
	} 
	
Results are of the form:

	"significant": {
			"_type": "significantTerms",
			"terms": [
				{
					"significance": 0.26,
					"sampleDf": 3,
					"term": "h5n1",
					"corpusDf": 4
				}...
			],
			"phrases": [
				{
					"terms": [
						"avian",
						"flu"
					],
					"popularity": 6
				},
		
### Next?

This is currently functional but being re-developed using the new aggregations API.

Mark Harwood
