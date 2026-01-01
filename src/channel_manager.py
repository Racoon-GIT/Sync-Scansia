"""
Modulo per gestione canali di pubblicazione prodotti Shopify.
"""
import logging
from typing import List, Optional

from .shopify_client import ShopifyClient
from .exceptions import ShopifyAPIError

LOG = logging.getLogger("sync.channel_manager")


def restrict_to_online_store_only(
    shop: ShopifyClient,
    product_gid: str,
    online_store_names: List[str] = None
) -> bool:
    """
    Rimuove prodotto da tutti i canali di vendita tranne "Negozio online".

    Args:
        shop: Client Shopify
        product_gid: GID prodotto
        online_store_names: Lista nomi publication da mantenere
                           Default: ["Online Store", "Negozio online"]

    Returns:
        True se successo, False altrimenti
    """
    if online_store_names is None:
        online_store_names = ["Online Store", "Negozio online", "Point of Sale"]

    LOG.info("Restrizione canali per prodotto: %s", product_gid)

    try:
        # Fetch tutti i canali disponibili
        publications = shop.get_publications()
        LOG.debug("Trovate %d publications totali", len(publications))

        # Identifica Online Store
        online_store_ids = []
        other_publication_ids = []

        for pub in publications:
            pub_id = pub.get("id")
            pub_name = pub.get("name", "")

            if any(name.lower() in pub_name.lower() for name in online_store_names):
                online_store_ids.append(pub_id)
                LOG.debug("Mantengo publication: %s (id: %s)", pub_name, pub_id)
            else:
                other_publication_ids.append(pub_id)
                LOG.debug("Da rimuovere publication: %s (id: %s)", pub_name, pub_id)

        if not online_store_ids:
            LOG.warning("Nessuna publication 'Online Store' trovata! Skip unpublish.")
            return False

        # Unpublish da tutti i canali tranne Online Store
        unpublished_count = 0
        for pub_id in other_publication_ids:
            try:
                shop.unpublish_from_publication(product_gid, pub_id)
                unpublished_count += 1
                LOG.info("Unpublished da publication: %s", pub_id)
            except Exception as e:
                # Non bloccare per singoli errori (potrebbe già essere unpublished)
                LOG.warning("Errore unpublish da publication %s: %s", pub_id, e)

        LOG.info("Restrizione canali completata: rimosso da %d/%d publication",
                 unpublished_count, len(other_publication_ids))
        return True

    except Exception as e:
        LOG.error("Errore restrizione canali: %s", e)
        return False
