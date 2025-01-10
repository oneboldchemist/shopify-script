import os
import re
import time
import json
import requests
import gspread
import psycopg2
from psycopg2.extras import RealDictCursor
from oauth2client.service_account import ServiceAccountCredentials

##############################################################################
#                     LÄS MILJÖVARIABLER (FÖR RENDER)                        #
##############################################################################

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")  # ex: "8bc028-b3.myshopify.com"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
LOCATION_ID = os.getenv("LOCATION_ID")
DATABASE_URL = os.getenv("DATABASE_URL")  # ex: "postgresql://tags_db_user:.../tags_db"
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not SHOP_DOMAIN:
    raise ValueError("Saknas env var: SHOP_DOMAIN")
if not SHOPIFY_ACCESS_TOKEN:
    raise ValueError("Saknas env var: SHOPIFY_ACCESS_TOKEN")
if not LOCATION_ID:
    raise ValueError("Saknas env var: LOCATION_ID")
if not DATABASE_URL:
    raise ValueError("Saknas env var: DATABASE_URL")
if not GOOGLE_CREDS_JSON:
    raise ValueError("Saknas env var: GOOGLE_CREDENTIALS_JSON")

##############################################################################
#                     KONFIG FÖR GOOGLE SHEETS (OAUTH)                       #
##############################################################################

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(GOOGLE_CREDS_JSON)
google_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(google_creds)

##############################################################################
#                          BYGG SHOPIFY-API BASE URL                         #
##############################################################################

BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/2023-07"

##############################################################################
#                          RELEVANTA TAGGAR (små/versaler)                    #
##############################################################################

RELEVANT_TAGS = {"male", "female", "unisex", "best seller", "bestseller"}

##############################################################################
#                       DB-FUNKTIONER FÖR ATT LÄSA/SKRIVA                     #
##############################################################################

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def load_tags_cache_db():
    """
    Hämtar ALLA (product_id, tags) från relevant_tags_cache i DB
    och returnerar en dict: { "1234567890": ["Male","BEST SELLER"], ... }
    """
    cache_dict = {}
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT product_id, tags FROM relevant_tags_cache;")
        rows = cur.fetchall()
        for row in rows:
            pid = row["product_id"]
            tags_str = row["tags"]
            tag_list = tags_str.split(",") if tags_str else []
            cache_dict[pid] = tag_list
    conn.close()
    return cache_dict

def save_tags_cache_db(cache_dict):
    """
    Uppdaterar relevanta taggar i DB (om ditt script lokalt behöver spara förändringar).
    cache_dict = {product_id: [tag1, tag2,...], ... }
    """
    conn = get_db_connection()
    with conn.cursor() as cur:
        for product_id, tag_list in cache_dict.items():
            tags_str = ",".join(tag_list)
            cur.execute("""
                INSERT INTO relevant_tags_cache (product_id, tags)
                VALUES (%s, %s)
                ON CONFLICT (product_id) DO UPDATE SET tags = EXCLUDED.tags;
            """, (product_id, tags_str))
    conn.commit()
    conn.close()

##############################################################################
#                    HJÄLPFUNKTIONER FÖR SHOPIFY OCH G SHEETS               #
##############################################################################

def safe_api_call(func, *args, **kwargs):
    try:
        response = func(*args, **kwargs)
        time.sleep(1)  # liten paus för att undvika rate limit
        return response
    except requests.exceptions.RequestException as e:
        print("[safe_api_call] Nätverksfel:", e)
        time.sleep(5)
        return safe_api_call(func, *args, **kwargs)

def normalize_minus_sign(value_str):
    if not value_str:
        return value_str
    return (value_str
            .replace('−', '-')
            .replace('\u2212', '-'))

def extract_perfume_number_from_product_title(product_title):
    pattern = r"\b(\d{1,3}(\.\d+)?)(?!\s*\d)"
    match = re.search(pattern, product_title)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def update_inventory_level(inventory_item_id, new_quantity):
    endpoint = f"{BASE_URL}/inventory_levels/set.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "location_id": LOCATION_ID,
        "inventory_item_id": inventory_item_id,
        "available": new_quantity
    }
    print(f"[update_inventory_level] => {endpoint}")
    print(f"   location_id={LOCATION_ID}, inventory_item_id={inventory_item_id}, available={new_quantity}")

    response = safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! Lagersaldo satt till {new_quantity}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

def update_product_tags(product_id, new_tag_list):
    """
    PUT /products/{product_id}.json
    new_tag_list ex: ["Male","BEST SELLER","Unisex"]
    """
    tags_str = ",".join(new_tag_list)
    endpoint = f"{BASE_URL}/products/{product_id}.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "product": {
            "id": product_id,
            "tags": tags_str
        }
    }
    print(f"[update_product_tags] => {endpoint}")
    print(f"   product_id={product_id}, tags='{tags_str}'")

    response = safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! Taggar uppdaterade: {new_tag_list}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

def fetch_all_products():
    print("[fetch_all_products] Hämtar alla produkter från Shopify...")
    all_products = []
    endpoint = f"{BASE_URL}/products.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    }
    params = {"limit": 250}

    while True:
        print(f"  -> GET {endpoint} (limit=250)")
        response = safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            products = data.get("products", [])
            all_products.extend(products)
            print(f"     Hämtade {len(products)} produkter, totalt {len(all_products)} nu.")

            link_header = response.headers.get("Link", "")
            next_link = None
            if 'rel="next"' in link_header:
                links = link_header.split(',')
                for part in links:
                    if 'rel="next"' in part:
                        next_link = part[part.find("<")+1:part.find(">")]
                        break
            if next_link:
                endpoint = next_link
                params = {}
            else:
                print("  -> Ingen fler sida hittades (rel=\"next\" saknas).")
                break
        else:
            print(f"[fetch_all_products] FEL {response.status_code}: {response.text}")
            break

    print(f"[fetch_all_products] Totalt {len(all_products)} produkter inlästa.\n")
    return all_products

##############################################################################
#                                   MAIN                                     #
##############################################################################

def main():
    """
    1. Ladda relevanta taggar från DB
    2. Hämta alla produkter (skippar "sample")
    3. Bygg perfume_lookup (parfymnr => produkt)
    4. Läs Google Sheet: "nummer:", "Antal:"
    5. Sätt lager i Shopify => hantera relevanta taggar (0 => ta bort, >0 => lägg till)
    6. (Om du ändrar relevant_tags_cache-lokalt) => save_tags_cache_db(...)
    """
    try:
        # 1) Hämta relevanta taggar från DB
        relevant_tags_cache = load_tags_cache_db()

        # 2) Hämta alla produkter
        all_products = fetch_all_products()

        perfume_lookup = {}
        print("[main] Bygger 'perfume_lookup' (skippar 'sample')...\n")
        for i, product in enumerate(all_products, start=1):
            product_id = str(product["id"])
            product_title = product.get("title", "")
            product_tags_str = product.get("tags", "")

            if "sample" in product_title.lower():
                print(f"  [Produkt #{i}] '{product_title}' innehåller 'sample', skippar.")
                continue

            # extrahera parfymnummer
            perfume_num = extract_perfume_number_from_product_title(product_title)
            if perfume_num is not None:
                perfume_lookup[perfume_num] = product
                print(f"  [Produkt #{i}] '{product_title}' (id={product_id}) => parfnum={perfume_num}, tags={product_tags_str}")
            else:
                print(f"  [Produkt #{i}] '{product_title}' => Ingen parfymnr-match.")

        print("\n[main] Klar med perfume_lookup!\n")

        # 3) Öppna Google Sheet
        print("[main] Öppnar Google Sheet 'OBC lager'...")
        sheet = client.open("OBC lager").sheet1
        rows = sheet.get_all_records()
        print(f"[main] Antal rader i kalkylarket: {len(rows)}\n")

        # 4) Loopar rader
        for idx, row in enumerate(rows, start=1):
            raw_num = str(row.get("nummer:", "")).strip()
            raw_antal = str(row.get("Antal:", "")).strip()

            print(f"--- [Rad #{idx}] ---------------------------------------------------")
            print(f"  => nummer: {raw_num}, Antal: {raw_antal}")

            if not raw_num or not raw_antal:
                print("  -> Ogiltig rad (saknar data). Hoppar.\n")
                continue

            raw_num = normalize_minus_sign(raw_num)
            raw_antal = normalize_minus_sign(raw_antal)

            try:
                num_float = float(raw_num)
                antal_int = int(raw_antal)
                if antal_int < 0:
                    print(f"  -> Antalet är negativt ({antal_int}), sätter 0.")
                    antal_int = 0
            except ValueError:
                print("  -> Kan ej tolka som siffror. Hoppar.\n")
                continue

            # 5) Finns parfymnr i perfume_lookup?
            if num_float in perfume_lookup:
                product_data = perfume_lookup[num_float]
                p_id = str(product_data["id"])
                product_variants = product_data.get("variants", [])
                product_tags_str = product_data.get("tags", "")

                print(f"  -> MATCH parfnum={num_float}, product_id={p_id}, sätter lager={antal_int}")

                # Uppdatera lager i Shopify
                for variant in product_variants:
                    inv_item_id = variant.get("inventory_item_id")
                    if inv_item_id:
                        update_inventory_level(inv_item_id, antal_int)
                    else:
                        print("    * Saknar inventory_item_id, hoppar.\n")

                # Hantera relevanta taggar
                current_tags_list = [t.strip() for t in product_tags_str.split(",") if t.strip()]
                original_db_tags = relevant_tags_cache.get(p_id, [])

                if antal_int == 0:
                    # => ta bort relevanta taggar
                    new_tags = []
                    for t in current_tags_list:
                        if t.lower() not in RELEVANT_TAGS:
                            new_tags.append(t)
                    if len(new_tags) != len(current_tags_list):
                        print(f"  -> Lager=0 => tar bort relevanta taggar => {new_tags}")
                        update_product_tags(p_id, new_tags)
                    else:
                        print("  -> Inga relevanta taggar att ta bort.")
                else:
                    # => lägg tillbaka relevanta taggar som fanns i DB
                    new_tags = current_tags_list[:]
                    changed = False
                    for rt in original_db_tags:
                        if rt not in new_tags:
                            new_tags.append(rt)
                            changed = True
                    if changed:
                        print(f"  -> Lager>0 => lägger tillbaka: {original_db_tags} => {new_tags}")
                        update_product_tags(p_id, new_tags)
                    else:
                        print("  -> Lager>0 => inga relevanta taggar saknades.")
            else:
                print(f"  -> Ingen produkt för parfnum={num_float}.\n")

        # 6) Om du lokalt uppdaterar relevant_tags_cache, kan du:
        #    save_tags_cache_db(relevant_tags_cache)

        print("\n[main] KLART – Scriptet har behandlat alla rader.\n")

    except Exception as e:
        print(f"Fel i main(): {e}")

if __name__ == "__main__":
    main()

