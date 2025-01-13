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
#                        ENV-VARIABLER FÖR DIN BUTIK & DB                    #
##############################################################################

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")           # ex: "8bc028-b3.myshopify.com"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
LOCATION_ID = os.getenv("LOCATION_ID")
DATABASE_URL = os.getenv("DATABASE_URL")         # ex: "postgresql://...@.../tags_db"
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not (SHOP_DOMAIN and SHOPIFY_ACCESS_TOKEN and LOCATION_ID and DATABASE_URL and GOOGLE_CREDENTIALS_JSON):
    raise ValueError("Saknas en eller flera ENV vars (SHOP_DOMAIN, SHOPIFY_ACCESS_TOKEN, LOCATION_ID, DATABASE_URL, GOOGLE_CREDENTIALS_JSON).")

BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/2023-07"

##############################################################################
#                         GOOGLE SHEETS-KONFIG (GSPREAD)                     #
##############################################################################

scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
google_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(google_creds)

##############################################################################
#              RELEVANTA TAGGAR & MAPPNING TILL "SERIER" (kollektioner)      #
##############################################################################

# Taggar du hanterar i DB (best seller, male, female, unisex)
RELEVANT_TAGS = {"male", "female", "unisex", "best seller", "bestseller"}

# Mappning: tag → "serie"-namn
SERIES_MAPPING = {
    "male": "men",
    "female": "women",
    "unisex": "unisex",
    "best seller": "bestsellers",
    "bestseller": "bestsellers"
}

# Kollektion-ID (dina "produktserier") i Shopify:
SERIES_COLLECTION_ID = {
    "men": 633426805078,        # https://admin.shopify.com/store/8bc028-b3/collections/633426805078
    "unisex": 633428934998,     # https://admin.shopify.com/store/8bc028-b3/collections/633428934998
    "women": 633426870614,      # https://admin.shopify.com/store/8bc028-b3/collections/633426870614
    "bestsellers": 626035360086 # https://admin.shopify.com/store/8bc028-b3/collections/626035360086
}

##############################################################################
#             DB-FUNKTION: LADDAR relevanta taggar (product_id -> tags)      #
##############################################################################

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def load_tags_cache_db():
    """
    Returnerar en dict { product_id (str): [tag1, tag2, ...], ... }
    från tabellen relevant_tags_cache i databasen.
    """
    cache_dict = {}
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT product_id, tags FROM relevant_tags_cache;")
        rows = cur.fetchall()
        for row in rows:
            pid = row["product_id"]
            tags_str = row["tags"] or ""
            tag_list = tags_str.split(",") if tags_str else []
            cache_dict[pid] = tag_list
    conn.close()
    return cache_dict

##############################################################################
#                   HJÄLPFUNKTION: BYGG "SERIES" UR TAGGAR                   #
##############################################################################

def build_series_list(tag_list):
    """
    Givet en lista av taggar, ex. ["Male","BEST SELLER","Unisex"],
    returnerar en lista av serier, ex. ["men","bestsellers","unisex"].
    """
    series_set = set()
    for t in tag_list:
        lower_t = t.lower()
        if lower_t in SERIES_MAPPING:
            series_set.add(SERIES_MAPPING[lower_t])
    return sorted(series_set)  # ex. ["bestsellers","men","unisex"]

##############################################################################
#                     SHOPIFY-API: TAGGAR, INVENTORY, KOLLEKTIONER           #
##############################################################################

def safe_api_call(func, *args, **kwargs):
    try:
        response = func(*args, **kwargs)
        time.sleep(1)
        return response
    except requests.exceptions.RequestException as e:
        print("[safe_api_call] Nätverksfel:", e)
        time.sleep(5)
        return safe_api_call(func, *args, **kwargs)

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

    resp = safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"   -> OK! Lagersaldo satt till {new_quantity}\n")
    else:
        print(f"   -> FEL! {resp.status_code}: {resp.text}\n")

def update_product_tags(product_id, new_tags_list):
    """
    PUT /products/{id}.json => "tags"
    """
    tags_str = ",".join(new_tags_list)
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

    resp = safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"   -> OK! Taggar uppdaterade: {new_tags_list}\n")
    else:
        print(f"   -> FEL! {resp.status_code}: {resp.text}\n")

##############################################################################
#        KOLLEKTION-API: LÄGG TILL / TA BORT EN PRODUKT UR EN KOLLEKTION     #
##############################################################################

def get_collections_for_product(product_id):
    """
    GET /collects.json?product_id=...
    Returnerar ex. { collection_id: collect_id, ... }
    """
    endpoint = f"{BASE_URL}/collects.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    params = {"product_id": product_id, "limit": 250}
    collects_map = {}
    while True:
        print(f"[get_collections_for_product] => GET {endpoint}?product_id={product_id}")
        resp = safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            collects = data.get("collects", [])
            for c in collects:
                cid = c["collection_id"]
                collects_map[cid] = c["id"]  # det unika collect-id:et
            # Kolla om mer data finns (paginering)
            link_header = resp.headers.get("Link", "")
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
                break
        else:
            print(f"   -> FEL! {resp.status_code}: {resp.text}")
            break
    return collects_map

def add_product_to_collection(product_id, collection_id):
    """
    POST /collects.json => 201 Created
    { "collect": { "product_id": <p>, "collection_id": <c> } }
    """
    endpoint = f"{BASE_URL}/collects.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "collect": {
            "product_id": product_id,
            "collection_id": collection_id
        }
    }
    print(f"[add_product_to_collection] => POST {endpoint}")
    print(f"   product_id={product_id}, collection_id={collection_id}")

    resp = safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if resp.status_code == 201:
        print("   -> OK! Lades till i kollektionen.\n")
    else:
        print(f"   -> FEL! {resp.status_code}: {resp.text}\n")

def remove_product_from_collection(collect_id):
    """
    DELETE /collects/{collect_id}.json => tar bort produkten ur kollektionen
    """
    endpoint = f"{BASE_URL}/collects/{collect_id}.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}

    print(f"[remove_product_from_collection] => DELETE {endpoint}")
    resp = safe_api_call(requests.delete, endpoint, headers=headers)
    if resp.status_code == 200:
        print("   -> OK! Tog bort produkten ur kollektionen.\n")
    else:
        print(f"   -> FEL! {resp.status_code}: {resp.text}\n")

def update_collections_for_product(product_id, new_series):
    """
    1) Hämta befintliga collects (kollektioner produkten redan ligger i).
    2) Bygg en set av "korrekta" collection_ids (enl. new_series + SERIES_COLLECTION_ID).
    3) Lägg till dem (POST) om saknas, ta bort (DELETE) om överskott.
    """
    existing_map = get_collections_for_product(product_id)  
      # ex. { 633426805078: <collectID1>, 626035360086: <collectID2> }
    wanted_ids = set()
    for s in new_series:
        c_id = SERIES_COLLECTION_ID.get(s)
        if c_id:
            wanted_ids.add(c_id)

    existing_ids = set(existing_map.keys())

    add_ids = wanted_ids - existing_ids
    remove_ids = existing_ids - wanted_ids

    if add_ids:
        print(f"   -> Lägga till i kollektioner: {list(add_ids)}")
        for cid in add_ids:
            add_product_to_collection(product_id, cid)
    else:
        print("   -> Inga nya kollektioner att lägga till.")

    if remove_ids:
        print(f"   -> Ta bort produkten ur kollektioner: {list(remove_ids)}")
        for cid in remove_ids:
            collect_id = existing_map[cid]
            remove_product_from_collection(collect_id)
    else:
        print("   -> Inga kollektioner att ta bort.")

##############################################################################
#                          HJÄLPFUNKTIONER FÖR SHEET ETC                     #
##############################################################################

def normalize_minus_sign(value_str):
    if not value_str:
        return value_str
    return (value_str
            .replace('−', '-')
            .replace('\u2212', '-'))

def extract_perfume_number_from_product_title(title):
    pattern = r"\b(\d{1,3}(\.\d+)?)(?!\s*\d)"
    match = re.search(pattern, title)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

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
        resp = safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            products = data.get("products", [])
            all_products.extend(products)
            print(f"     Hämtade {len(products)} st, totalt: {len(all_products)}")

            link_header = resp.headers.get("Link", "")
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
            print(f"   -> FEL! {resp.status_code}: {resp.text}")
            break

    print(f"[fetch_all_products] Totalt {len(all_products)} produkter inlästa.\n")
    return all_products

##############################################################################
#                                   MAIN                                     #
##############################################################################

def main():
    try:
        # 1) Ladda relevanta taggar från DB
        relevant_tags_cache = load_tags_cache_db()

        # 2) Hämta alla Shopify-produkter
        all_products = fetch_all_products()

        # Bygg en perfume_lookup => { parfymnr: product_dict }
        perfume_lookup = {}
        print("[main] Bygger 'perfume_lookup' (skippar 'sample' i titeln)...\n")
        for i, product in enumerate(all_products, start=1):
            product_id = str(product["id"])
            title = product.get("title", "")
            if "sample" in title.lower():
                print(f" [Prod #{i}] '{title}' => 'sample', skippar.")
                continue
            parfnum = extract_perfume_number_from_product_title(title)
            if parfnum is not None:
                perfume_lookup[parfnum] = product
                print(f" [Prod #{i}] '{title}' => parfnum={parfnum}, product_id={product_id}")
            else:
                print(f" [Prod #{i}] '{title}' => ingen parfymnr-match.")

        # 3) Öppna Google Sheet "OBC lager"
        print("\n[main] Öppnar Google Sheet 'OBC lager'...")
        sheet = client.open("OBC lager").sheet1
        rows = sheet.get_all_records()
        print(f"[main] Antal rader i kalkylarket: {len(rows)}\n")

        # 4) Loopa rader
        for idx, row in enumerate(rows, start=1):
            raw_num = str(row.get("nummer:", "")).strip()
            raw_antal = str(row.get("Antal:", "")).strip()

            print(f"--- [Rad #{idx}] ---------------------------------------------------")
            print(f"   nummer: {raw_num}, Antal: {raw_antal}")

            if not raw_num or not raw_antal:
                print("   -> Ogiltig rad, hoppar.\n")
                continue

            raw_num = normalize_minus_sign(raw_num)
            raw_antal = normalize_minus_sign(raw_antal)

            try:
                num_float = float(raw_num)
                antal_int = int(raw_antal)
                if antal_int < 0:
                    antal_int = 0
            except ValueError:
                print("   -> Kan ej tolka som siffror, hoppar.\n")
                continue

            # Finns parfnum i perfume_lookup?
            if num_float in perfume_lookup:
                product_data = perfume_lookup[num_float]
                p_id = str(product_data["id"])
                variants = product_data.get("variants", [])

                print(f"   -> MATCH parfnum={num_float}, product_id={p_id}, lager={antal_int}")

                # (A) Uppdatera lager i Shopify
                for var in variants:
                    inv_item_id = var.get("inventory_item_id")
                    if inv_item_id:
                        update_inventory_level(inv_item_id, antal_int)

                # (B) "Relevanta" taggar i DB
                db_tags = relevant_tags_cache.get(p_id, [])  # ex. ["Male","BEST SELLER"]
                # Bygg vilka "serier" => ex. ["men","bestsellers"] 
                series_list = build_series_list(db_tags)

                # (C) Kolla nuvarande "tags" i Shopify
                shopify_tags_str = product_data.get("tags","")
                shopify_tags_list = [t.strip() for t in shopify_tags_str.split(",") if t.strip()]

                if antal_int == 0:
                    # => Ta bort relevanta taggar
                    new_tags = []
                    for t in shopify_tags_list:
                        if t.lower() not in RELEVANT_TAGS:
                            new_tags.append(t)
                    if len(new_tags) != len(shopify_tags_list):
                        print(f"   -> Lager=0 => ta bort relev. taggar => {new_tags}")
                        update_product_tags(p_id, new_tags)
                    else:
                        print("   -> Inga relevanta taggar att ta bort.")

                    # => Ta bort ur kollektioner
                    print(f"   -> Lager=0 => tar bort ur kollektioner.")
                    update_collections_for_product(p_id, [])  # tom lista
                else:
                    # => LÄGG TILLBAKA relevanta taggar
                    new_tags = shopify_tags_list[:]
                    changed = False
                    for rt in db_tags:
                        if rt not in new_tags:
                            new_tags.append(rt)
                            changed = True
                    if changed:
                        print(f"   -> Lager>0 => lägger tillbaka taggar => {new_tags}")
                        update_product_tags(p_id, new_tags)
                    else:
                        print("   -> Lager>0 => inga relevanta taggar saknades.")

                    # => LÄGG TILL kollektioner
                    if series_list:
                        print(f"   -> Lager>0 => uppdatera kollektioner => {series_list}")
                        update_collections_for_product(p_id, series_list)
                    else:
                        print("   -> Lager>0 => inga relevanta kollektioner.")
            else:
                print(f"   -> Ingen produkt för parfnum={num_float}.\n")

        print("\n[main] KLART – Scriptet har behandlat alla rader i Google Sheet.\n")

    except Exception as e:
        print(f"Fel i main(): {e}")

if __name__ == "__main__":
    main()

