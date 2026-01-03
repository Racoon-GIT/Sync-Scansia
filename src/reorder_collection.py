#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reorder_collection.py - Ordina prodotti collection per sconto percentuale

Usage:
    python reorder_collection.py --collection-id 262965428289 --apply

ENV richieste:
  SHOPIFY_STORE
  SHOPIFY_ADMIN_TOKEN
  SHOPIFY_API_VERSION (default: 2025-01)
"""

import argparse
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("reorder")

class ShopifyCollectionReorder:
    def __init__(self):
        # Supporta SHOPIFY_STORE o usa default hardcoded
        self.store = os.environ.get("SHOPIFY_STORE") or "racoon-lab.myshopify.com"
        self.token = os.environ.get("SHOPIFY_ADMIN_TOKEN")
        self.api_version = os.environ.get("SHOPIFY_API_VERSION", "2025-01")

        if not self.token:
            raise RuntimeError("SHOPIFY_ADMIN_TOKEN environment variable not set")
        self.base = f"https://{self.store}/admin/api/{self.api_version}"
        self.graphql_url = f"{self.base}/graphql.json"
        
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
        })
        
        self.min_interval = float(os.environ.get("SHOPIFY_MIN_INTERVAL_SEC", "0.7"))
        self.max_retries = int(os.environ.get("SHOPIFY_MAX_RETRIES", "5"))
        self._last_call_ts = 0.0
    
    def _throttle(self):
        """Rate limiting"""
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
    
    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """GraphQL request con retry automatico"""
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            
            try:
                r = self.sess.post(
                    self.graphql_url, 
                    json={"query": query, "variables": variables},
                    timeout=30  # Timeout 30s
                )
                self._last_call_ts = time.time()
                
                # Gestione 429 - Rate limit exceeded
                if r.status_code == 429:
                    retry_after = float(r.headers.get("Retry-After", 2.0))
                    logger.warning(f"429 Rate limit (tentativo {attempt}/{self.max_retries}). Retry in {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                # Gestione 5xx - Server errors
                if 500 <= r.status_code < 600:
                    backoff = min(2 ** (attempt - 1), 8)
                    logger.warning(f"Server error {r.status_code} (tentativo {attempt}/{self.max_retries}). Retry in {backoff}s")
                    time.sleep(backoff)
                    continue
                
                # Altri errori HTTP
                if r.status_code >= 400:
                    error_text = r.text[:200] if r.text else "No response body"
                    raise RuntimeError(f"GraphQL HTTP {r.status_code}: {error_text}")
                
                # Parse response
                data = r.json()
                
                # Gestione errori GraphQL
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")
                
                return data["data"]
                
            except requests.exceptions.Timeout:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning(f"Timeout (tentativo {attempt}/{self.max_retries}). Retry in {backoff}s")
                time.sleep(backoff)
                continue
            
            except requests.exceptions.RequestException as e:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning(f"Request error (tentativo {attempt}/{self.max_retries}): {e}. Retry in {backoff}s")
                time.sleep(backoff)
                continue
        
        # Se arriviamo qui, tutti i retry sono falliti
        raise RuntimeError(f"GraphQL failed after {self.max_retries} tentativi")
    
    def get_collection_products(self, collection_gid: str) -> List[Dict[str, Any]]:
        """
        Ottiene tutti i prodotti di una collection con prezzi.
        Restituisce lista di prodotti con prima variante (per calcolo sconto).
        """
        logger.info("Recupero prodotti dalla collection...")
        
        products = []
        cursor = None
        page = 0
        
        while True:
            page += 1
            after_clause = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            query {{
              collection(id: "{collection_gid}") {{
                products(first: 50{after_clause}) {{
                  pageInfo {{
                    hasNextPage
                    endCursor
                  }}
                  edges {{
                    node {{
                      id
                      title
                      handle
                      variants(first: 1) {{
                        edges {{
                          node {{
                            id
                            price
                            compareAtPrice
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
            """
            
            data = self.graphql(query, {})
            
            collection = data.get("collection")
            if not collection:
                raise RuntimeError(f"Collection {collection_gid} non trovata")
            
            edges = collection["products"]["edges"]
            logger.info(f"Pagina {page}: {len(edges)} prodotti")
            
            for edge in edges:
                product = edge["node"]
                
                # Estrai prima variante per prezzi
                variant_edges = product["variants"]["edges"]
                if not variant_edges:
                    logger.warning(f"Prodotto {product['id']} senza varianti, skip")
                    continue
                
                variant = variant_edges[0]["node"]
                
                products.append({
                    "id": product["id"],
                    "title": product["title"],
                    "handle": product["handle"],
                    "price": float(variant["price"]),
                    "compare_at_price": float(variant["compareAtPrice"]) if variant["compareAtPrice"] else None
                })
            
            # Paginazione
            page_info = collection["products"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
        
        logger.info(f"Totale prodotti recuperati: {len(products)}")
        return products
    
    def calculate_discount_percentage(self, product: Dict[str, Any]) -> float:
        """
        Calcola sconto percentuale: (compareAtPrice - price) / compareAtPrice * 100
        Restituisce 0.0 se non c'è compareAtPrice.
        """
        compare_at = product.get("compare_at_price")
        price = product.get("price")
        
        if not compare_at or compare_at <= 0:
            return 0.0
        
        if price >= compare_at:
            return 0.0
        
        discount = ((compare_at - price) / compare_at) * 100
        return round(discount, 2)
    
    def sort_by_discount(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ordina prodotti per sconto percentuale decrescente.
        Prodotti con stesso sconto mantengono ordine alfabetico per titolo.
        """
        logger.info("Calcolo sconti e ordinamento...")
        
        # Aggiungi sconto % a ogni prodotto
        for p in products:
            p["discount_pct"] = self.calculate_discount_percentage(p)
        
        # Ordina: prima per sconto DESC, poi per titolo ASC
        sorted_products = sorted(
            products,
            key=lambda x: (-x["discount_pct"], x["title"].lower())
        )
        
        # Log riepilogo
        logger.info("Primi 10 prodotti dopo ordinamento:")
        for i, p in enumerate(sorted_products[:10], 1):
            logger.info(f"  {i}. {p['title'][:50]:50} - Sconto: {p['discount_pct']:5.1f}%")
        
        return sorted_products
    
    def reorder_collection(self, collection_gid: str, ordered_product_ids: List[str]):
        """
        Riordina prodotti nella collection usando GraphQL collectionReorderProducts.
        
        La mutation richiede "moves" che specificano le posizioni.
        """
        logger.info("Riordino collection su Shopify...")
        
        # Costruisci moves: ogni prodotto va alla sua posizione nell'array
        # moves = [{ id: productId, newPosition: "0" }, ...]
        moves = []
        for idx, product_id in enumerate(ordered_product_ids):
            moves.append({
                "id": product_id,
                "newPosition": str(idx)
            })
        
        # Esegui mutation in batch (max 250 moves per volta)
        batch_size = 250
        job_ids = []
        
        for i in range(0, len(moves), batch_size):
            batch = moves[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(moves) + batch_size - 1) // batch_size
            
            logger.info(f"Riordino batch {batch_num}/{total_batches}: {len(batch)} prodotti")
            
            mutation = """
            mutation collectionReorderProducts($id: ID!, $moves: [MoveInput!]!) {
              collectionReorderProducts(id: $id, moves: $moves) {
                job {
                  id
                  done
                }
                userErrors {
                  field
                  message
                }
              }
            }
            """
            
            variables = {
                "id": collection_gid,
                "moves": batch
            }
            
            data = self.graphql(mutation, variables)
            
            result = data["collectionReorderProducts"]
            
            if result["userErrors"]:
                logger.error(f"Errori reorder batch {batch_num}: {result['userErrors']}")
                raise RuntimeError(f"Reorder fallito: {result['userErrors']}")
            
            job = result.get("job")
            if job:
                job_ids.append(job["id"])
                logger.info(f"Job creato: {job['id']}, done: {job['done']}")
            
            # Delay tra batch per non saturare API
            if i + batch_size < len(moves):
                logger.debug(f"Pausa 1s prima del prossimo batch...")
                time.sleep(1.0)
        
        # Aspetta completamento job (se ci sono job pendenti)
        if job_ids:
            logger.info(f"Attendo completamento {len(job_ids)} job...")
            self._wait_for_jobs(job_ids)
        
        logger.info("✅ Riordino completato")
    
    def _wait_for_jobs(self, job_ids: List[str], max_wait_sec: int = 60):
        """
        Aspetta che i job siano completati.
        Polling ogni 2 secondi fino a completamento o timeout.
        """
        start_time = time.time()
        pending_jobs = set(job_ids)
        
        while pending_jobs and (time.time() - start_time) < max_wait_sec:
            time.sleep(2.0)
            
            for job_id in list(pending_jobs):
                try:
                    query = """
                    query($id: ID!) {
                      job(id: $id) {
                        id
                        done
                      }
                    }
                    """
                    
                    data = self.graphql(query, {"id": job_id})
                    job = data.get("job")
                    
                    if job and job["done"]:
                        logger.info(f"✓ Job completato: {job_id}")
                        pending_jobs.remove(job_id)
                    else:
                        logger.debug(f"Job {job_id} ancora in esecuzione...")
                        
                except Exception as e:
                    logger.warning(f"Errore check job {job_id}: {e}")
                    # Rimuovi comunque per evitare loop infinito
                    pending_jobs.remove(job_id)
        
        if pending_jobs:
            logger.warning(f"⚠️  Timeout: {len(pending_jobs)} job ancora pendenti dopo {max_wait_sec}s")
            logger.warning(f"Job pendenti: {list(pending_jobs)}")
        else:
            logger.info(f"✅ Tutti i job completati in {time.time() - start_time:.1f}s")

def main():
    parser = argparse.ArgumentParser(description="Riordina collection per sconto %")
    parser.add_argument("--collection-id", required=True, help="ID numerico collection (es: 262965428289)")
    parser.add_argument("--apply", action="store_true", help="Applica riordino (altrimenti dry-run)")
    args = parser.parse_args()
    
    # Converti ID numerico in GID
    collection_gid = f"gid://shopify/Collection/{args.collection_id}"
    
    logger.info("=" * 70)
    logger.info("REORDER COLLECTION BY DISCOUNT %")
    logger.info(f"Collection ID: {args.collection_id}")
    logger.info(f"Collection GID: {collection_gid}")
    logger.info(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    logger.info("=" * 70)
    
    # Inizializza client
    shop = ShopifyCollectionReorder()
    
    # 1. Ottieni prodotti
    products = shop.get_collection_products(collection_gid)
    
    if not products:
        logger.warning("Nessun prodotto trovato nella collection")
        return
    
    # 2. Ordina per sconto
    sorted_products = shop.sort_by_discount(products)
    
    # 3. Report
    logger.info("=" * 70)
    logger.info("RIEPILOGO ORDINAMENTO:")
    logger.info(f"Totale prodotti: {len(sorted_products)}")
    
    discount_counts = {}
    for p in sorted_products:
        discount = int(p["discount_pct"])
        discount_counts[discount] = discount_counts.get(discount, 0) + 1
    
    logger.info("Distribuzione sconti:")
    for discount in sorted(discount_counts.keys(), reverse=True):
        count = discount_counts[discount]
        logger.info(f"  {discount}%: {count} prodotti")
    
    # 4. Applica riordino
    if args.apply:
        logger.info("=" * 70)
        ordered_ids = [p["id"] for p in sorted_products]
        shop.reorder_collection(collection_gid, ordered_ids)
        logger.info("=" * 70)
        logger.info("✅ FATTO! Collection riordinata per sconto % decrescente")
    else:
        logger.info("=" * 70)
        logger.info("⚠️  DRY-RUN: Usa --apply per applicare riordino")

if __name__ == "__main__":
    main()
