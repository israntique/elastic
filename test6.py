import json
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "12345678"
es = Elasticsearch("http://localhost:9200",
                   basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),  # Authentication credentials
                   verify_certs=True)
#print(es.info())


# Define the index name
index_name = "entities"

# Corrected mapping structure
mapping = {
  "settings": {
    "analysis": {
      "filter": {
        "autocomplete_filter": {
          "type": "ngram",      # <-- not edge_ngram
          "min_gram": 2,
          "max_gram": 20
        }
      },
      "analyzer": {
        "autocomplete_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "autocomplete_filter"
          ]
        },
        "autocomplete_search_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "entity_id": { "type": "long" },
      "txt": {
        "type": "nested",
        "properties": {
          "entity_text_string": {
            "type": "text",
            "analyzer": "autocomplete_analyzer",
            "search_analyzer": "autocomplete_search_analyzer",
            "fields": {
              "keyword": { "type": "keyword" },
              "fulltext": { "type": "text" }
            }
          }
        }
      }
    }
  }
}

# Check if the index already exists
if not es.indices.exists(index=index_name):
    # Create the index with the mapping
    try:
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created successfully with mapping.")
    except Exception as e:
        print(f"Error creating index '{index_name}': {e}")
else:
    print(f"Index '{index_name}' already exists.")
    # You might want to update the mapping if it's different from what you expect
    #  es.indices.put_mapping(index=index_name, body=mapping['mappings']) #update the mapping
    #  print(f"Index '{index_name}' already exists.  Mapping not updated.")
    pass


# Load JSON data from file
with open("data.json", "r", encoding="utf-8") as file:
    data = json.load(file)
    print('after load data')
# Index the documents
for doc in data:
    es.index(index=index_name, id=doc["entity_id"], body=doc)

print("Documents indexed successfully!")

#search_text = "שלחת"
#search_text = "משלחת לבד"
# Build the nested match_phrase query
search_text = "משלחת"

query = {
  "query": {
    "nested": {
      "path": "txt",
      "query": {
        "bool": {
          "should": [
            {
              "match": {
                "txt.entity_text_string": {
                  "query": search_text,
                  "operator": "and"
                }
              }
            },
            {
              "match_phrase": {
                "txt.entity_text_string": {
                  "query": search_text,
                  "slop": 2
                }
              }
            }
          ]
        }
      },
      "inner_hits": {}
    }
  }
}

query_1 = {
  "query": {
    "nested": {
      "path": "txt",
      "query": {
        "bool": {
          "should": [  # OR logic: either of these matches
            {
              "match": {
                "txt.entity_text_string": {
                  "query": search_text,
                  "operator": "and"   # All words must appear (partial allowed)
                }
              }
            },
            {
              "match_phrase": {
                "txt.entity_text_string": {
                  "query": search_text,
                  "slop": 2   # Allow up to 2 words in between
                }
              }
            }
          ]
        }
      },
      "inner_hits": {}  # to see which part matched
    }
  }
}



# Execute search query
response = es.search(index=index_name, body=query)
# Print entity_ids of matching documents
#print("\nEntity IDs containing 'משלחת לבד':")
print("\nEntity IDs containing "+search_text)
for hit in response['hits']['hits']:
    entity_id = hit['_source']['entity_id']
    print(f"Found entity_id: {entity_id}")
