import os
import json
from elasticsearch import Elasticsearch, helpers, exceptions

# ─── Config ────────────────────────────────────────────────────────────────────
ES_HOST     = "http://localhost:9200"
ES_USER     = "elastic"
ES_PASSWORD = "12345678"
INDEX_NAME  = "entities2"
DATA_DIR    = "entities"   # folder with entity1.json, entity2.json, …

# ─── Mapping ───────────────────────────────────────────────────────────────────
MAPPING = {
    "settings": {
            "index": {
            "max_ngram_diff": 18    # allow max_gram - min_gram up to 18
            },
        "analysis": {
            "filter": {
                "autocomplete_filter": {
                    "type":     "ngram",
                    "min_gram": 2,
                    "max_gram": 3
                }
            },
            "analyzer": {
                "autocomplete_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase","autocomplete_filter"]
                },
                "autocomplete_search_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase"]
                }
            }
        }
    },
    "mappings": {
  "properties": {
    "entity_id": {
      "type": "integer"
    },
    "origin_id": {
      "type": "keyword"
    },
    "origin_table": {
      "type": "keyword"
    },
    "licenses": {
      "type": "nested",
      "properties": {
        "license_id": {
          "type": "integer"
        }
      }
    },
    "details": {
      "properties": {
        "title": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "description": {
          "type": "text"
        }
      }
    },
    "thesaurus": {
      "type": "nested",
      "properties": {
        "wid": {
          "type": "integer"
        },
        "cod_thes": {
          "type": "keyword"
        }
      }
    },
    "texts": {
      "type": "nested",
      "properties": {
        "text_id": {
          "type": "integer"
        },
        "type": {
          "type": "keyword"
        },
        "entity_text_string": {
          "type": "text"
        },
        "subtype": {
          "type": "keyword"
        }
      }
    },
    "shapes": {
      "type": "nested",
      "properties": {
        "shape_id": {
          "type": "integer"
        },
        "shape": {
          "type": "text"
        },
        "shape_wkt": {
          "type": "text"
        }
      }
    }
  }
}

}

# ─── Helpers ───────────────────────────────────────────────────────────────────
def ensure_index(es):
    """
    Check for the index’s existence and create it if missing.
    """
    try:
        exists = es.indices.exists(index=INDEX_NAME)
    except exceptions.ConnectionError as e:
        print("✖ Could not check index existence:", e)
        raise

    if not exists:
        es.indices.create(index=INDEX_NAME, body=MAPPING)
        print(f"✔ Created index '{INDEX_NAME}'")
    else:
        print(f"→ Index '{INDEX_NAME}' already exists")
def load_all_docs(folder):
    docs = []
    for fname in os.listdir(folder):
        if fname.endswith(".json"):
            with open(os.path.join(folder, fname), "r", encoding="utf-8") as f:
                docs.append(json.load(f))
    print(f"Loaded {len(docs)} documents from '{folder}'")
    return docs

def bulk_index(es, docs):
    actions = [
        {
            "_op_type": "index",
            "_index": INDEX_NAME,
            "_id": doc["entity_id"],
            "_source": doc
        }
        for doc in docs
    ]
    helpers.bulk(es, actions)
    print(f"✔ Bulk-indexed {len(docs)} docs into '{INDEX_NAME}'")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=True
    )

    ensure_index(es)
    docs = load_all_docs(DATA_DIR)
    if docs:
        bulk_index(es, docs)
    else:
        print("⚠️  No JSON files found to index.")

if __name__ == "__main__":
    main()
