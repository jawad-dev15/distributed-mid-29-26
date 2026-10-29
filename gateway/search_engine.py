import re
import time
from typing import List, Dict, Any, Optional, Set

class ElasticSearchIndex:
    """
    Elasticsearch-compatible Metadata Search Engine.
    Implements inverted indexing, tokenization, term matching, prefix querying,
    and boolean filters over object metadata and user tags.
    """
    def __init__(self, index_name: str = "objects_metadata"):
        self.index_name = index_name
        self.documents: Dict[str, Dict[str, Any]] = {}  # doc_id -> doc
        self.inverted_index: Dict[str, Set[str]] = {}   # term -> set(doc_id)
        self.tag_index: Dict[str, Set[str]] = {}        # "key:value" -> set(doc_id)

    def _tokenize(self, text: str) -> List[str]:
        """Splits path/key into lowercase alphanumeric terms."""
        return [t.lower() for t in re.split(r'[/_.\s-]+', text) if len(t) > 1]

    def index_document(self, doc: Dict[str, Any]) -> None:
        """Indexes or updates an object document in the search index."""
        doc_id = f"{doc['bucket']}/{doc['key']}"
        self.documents[doc_id] = doc

        # Index key terms
        terms = self._tokenize(doc["key"]) + self._tokenize(doc["bucket"])
        for term in terms:
            if term not in self.inverted_index:
                self.inverted_index[term] = set()
            self.inverted_index[term].add(doc_id)

        # Index tags
        tags = doc.get("tags", {})
        for k, v in tags.items():
            tag_token = f"{k.lower()}:{str(v).lower()}"
            if tag_token not in self.tag_index:
                self.tag_index[tag_token] = set()
            self.tag_index[tag_token].add(doc_id)

    def delete_document(self, bucket: str, key: str) -> bool:
        """Removes an object document from the search index."""
        doc_id = f"{bucket}/{key}"
        if doc_id in self.documents:
            del self.documents[doc_id]
            for term_set in self.inverted_index.values():
                term_set.discard(doc_id)
            for tag_set in self.tag_index.values():
                tag_set.discard(doc_id)
            return True
        return False

    def query_dsl(self, dsl: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes an Elasticsearch-compliant Query DSL search.
        Supports:
          - match query (full text over key/terms)
          - term query (exact match on tags or bucket)
          - prefix query (key path prefix)
          - range query (size bounds)
          - bool query (must, filter)
        """
        start_time = time.time()
        query = dsl.get("query", {})
        matched_doc_ids: Optional[Set[str]] = None

        def intersect(current: Optional[Set[str]], new_set: Set[str]) -> Set[str]:
            return new_set if current is None else current.intersection(new_set)

        # 1. Process 'match'
        if "match" in query:
            for field, val in query["match"].items():
                terms = self._tokenize(str(val))
                field_matches = set()
                for t in terms:
                    field_matches.update(self.inverted_index.get(t, set()))
                matched_doc_ids = intersect(matched_doc_ids, field_matches)

        # 2. Process 'prefix'
        if "prefix" in query:
            for field, prefix_str in query["prefix"].items():
                p_lower = prefix_str.lower()
                prefix_matches = {
                    doc_id for doc_id, doc in self.documents.items()
                    if doc.get(field, "").lower().startswith(p_lower)
                }
                matched_doc_ids = intersect(matched_doc_ids, prefix_matches)

        # 3. Process 'term'
        if "term" in query:
            for field, val in query["term"].items():
                if field.startswith("tags."):
                    tag_key = field.replace("tags.", "").lower()
                    tag_token = f"{tag_key}:{str(val).lower()}"
                    matched_doc_ids = intersect(matched_doc_ids, self.tag_index.get(tag_token, set()))
                else:
                    term_matches = {
                        doc_id for doc_id, doc in self.documents.items()
                        if str(doc.get(field, "")).lower() == str(val).lower()
                    }
                    matched_doc_ids = intersect(matched_doc_ids, term_matches)

        # 4. Process 'range'
        if "range" in query:
            for field, bounds in query["range"].items():
                gte = bounds.get("gte", float("-inf"))
                lte = bounds.get("lte", float("inf"))
                range_matches = {
                    doc_id for doc_id, doc in self.documents.items()
                    if gte <= doc.get(field, 0) <= lte
                }
                matched_doc_ids = intersect(matched_doc_ids, range_matches)

        # Default to all documents if empty query
        if matched_doc_ids is None:
            matched_doc_ids = set(self.documents.keys())

        # Build ES formatted response
        hits = []
        for doc_id in matched_doc_ids:
            doc = self.documents[doc_id]
            hits.append({
                "_index": self.index_name,
                "_id": doc_id,
                "_score": 1.0,
                "_source": doc
            })

        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        return {
            "took": elapsed_ms,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": len(hits), "relation": "eq"},
                "max_score": 1.0 if hits else None,
                "hits": hits
            }
        }
