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
#                           ENV VARS FÖR RENDER                              #
##############################################################################

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
LOCATION_ID = os.getenv("LOCATION_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not SHOP_DOMAIN or not SHOPIFY_ACCESS_TOKEN or not LOCATION_ID or not DATABASE_URL or not GOOGLE_CREDS_JSON:
    raise ValueError("Saknas en eller flera environment variables (SHOP_DOMAIN, SHOPIFY_ACCESS_TOKEN, LOCATION_ID, DATABASE_URL, GOOGLE_CREDENTIALS_JSON).")

BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/2023-07"

##############################################################################
#                         GOOGLE SHEETS KONFIG                                #
##############################################################################

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(GOOGLE_CREDS_JSON)
google_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(google_creds)

##############################################################################
#                     RELEVANTA TAGGAR OCH SERIER-MAPPING                    #
##############################################################################

RELEVANT_TAGS = {"male", "female", "unisex", "best seller", "bestseller"}

SERIES_MAPPING = {
    "male": "men",
    "female": "women",
    "unisex": "unisex",
    "best seller": "bestsellers",
    "bestseller": "bestsellers"
}

##############################################################################
#                       DB-FUNKTIONER FÖR TAG-CACHE                           #
##############################################################################

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def load_tags_cache_db():
    """
    Ladda en dict { product_id: [tag1, tag2, ...], ... }
    från tabellen relevant_tags_cache.
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

##############################################################################
#             HJÄLPFUNKTION FÖR ATT BYGGA "SERIES" UTIFRÅN TAGGAR            #
##############################################################################

def build_series_list(tag_list):
    """
    Givet en lista av taggar, t.ex. ["Male","BEST SELLER","Unisex"],
    returnerar en lista av serier, ex. ["men","bestsellers","unisex"].
    """
    series_set = set()
    for t in tag_list:
        lower_t = t.lower()
        if lower_t in SERIES_MAPPING:
            series_set.add(SERIES_MAPPING[lower_t])
    return sorted(series_set)  # ex. ["bestsellers","men","unisex"]

##############################################################################
#             FUNKTIONER FÖR ATT UPPDATERA TAGGAR OCH SERIER I SHOPIFY       #
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

    response = safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! Lagersaldo satt till {new_quantity}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

def update_product_tags(product_id, new_tag_list):
    """
    PUT /products/{id}.json => "tags"
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
    print(f"[update_product_tags] => PUT {endpoint}")
    print(f"   product_id={product_id}, tags='{tags_str}'")

    response = safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! Taggar uppdaterade: {new_tag_list}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

def update_product_series(product_id, series_list):
    """
    PUT /products/{id}.json => "product_type"
    Ex. series_list=["men","unisex"] => product_type="men,unisex"
    """
    series_str = ",".join(series_list)
    endpoint = f"{BASE_URL}/products/{product_id}.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "product": {
            "id": product_id,
            "product_type": series_str
        }
    }
    print(f"[update_product_series] => PUT {endpoint}")
    print(f"   product_id={product_id}, product_type='{series_str}'")

    response = safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! product_type uppdaterat: {series_list}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

##############################################################################
#                   GOOGLE SHEETS & STÖD-FUNKTIONER                          #
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
    try:
        # 1) Hämta relevanta taggar (cache) från DB
        relevant_tags_cache = load_tags_cache_db()

        # 2) Hämta alla produkter
        all_products = fetch_all_products()

        perfume_lookup = {}
        print("[main] Bygger 'perfume_lookup' (skippar 'sample')...\n")
        for i, product in enumerate(all_products, start=1):
            product_id = str(product["id"])
            product_title = product.get("title", "")
            product_tags_str = product.get("tags", "")

            # Skip "sample" i titeln
            if "sample" in product_title.lower():
                print(f"  [Prod #{i}] '{product_title}' => innehåller 'sample', skippar.")
                continue

            # extrahera parfymnr
            parfnum = extract_perfume_number_from_product_title(product_title)
            if parfnum is not None:
                perfume_lookup[parfnum] = product
                print(f"  [Prod #{i}] '{product_title}' (id={product_id}) => parfnum={parfnum}, tags={product_tags_str}")
            else:
                print(f"  [Prod #{i}] '{product_title}' => Ingen parfymnr-match.")

        print("\n[main] Klar med perfume_lookup!\n")

        # 3) Öppna Google Sheet “OBC lager”
        print("[main] Öppnar Google Sheet 'OBC lager'...")
        sheet = client.open("OBC lager").sheet1
        rows = sheet.get_all_records()
        print(f"[main] Antal rader i kalkylarket: {len(rows)}\n")

        # 4) Loopa rader i sheet
        for idx, row in enumerate(rows, start=1):
            raw_num = str(row.get("nummer:", "")).strip()
            raw_antal = str(row.get("Antal:", "")).strip()

            print(f"--- [Rad #{idx}] ---------------------------------------------------")
            print(f"  => nummer: {raw_num}, Antal: {raw_antal}")

            if not raw_num or not raw_antal:
                print("  -> Ogiltig rad (saknar nummer/Antal). Hoppar.\n")
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

            # 5) Finns parfnum i perfume_lookup?
            if num_float in perfume_lookup:
                product_data = perfume_lookup[num_float]
                p_id = str(product_data["id"])
                product_variants = product_data.get("variants", [])
                current_shopify_tags_str = product_data.get("tags", "")

                print(f"  -> MATCH parfnum={num_float}, product_id={p_id}, sätter lager={antal_int}")

                # (A) Uppdatera lager i Shopify
                for variant in product_variants:
                    inv_item_id = variant.get("inventory_item_id")
                    if inv_item_id:
                        update_inventory_level(inv_item_id, antal_int)
                    else:
                        print("    * Saknar inventory_item_id. Hoppar.\n")

                # (B) Taggar (Shopify)
                current_tags_list = [t.strip() for t in current_shopify_tags_str.split(",") if t.strip()]
                # ...tags i DB (relevanta)
                original_db_tags = relevant_tags_cache.get(p_id, [])  

                # (C) Serie-lista baserat på DB-taggar
                # ex. om original_db_tags = ["Male","BEST SELLER"], => series = ["men","bestsellers"]
                # (Vi vill att "lager=0" => ta bort, "lager>0" => lägg tillbaka)
                # men för att "lägga tillbaka" en serie, måste taggen finnas i DB.
                # => build_series_list(original_db_tags)
                current_series_list = build_series_list(original_db_tags)

                if antal_int == 0:
                    # => Ta bort relevanta taggar ur Shopify "tags"
                    new_tags = []
                    for t in current_tags_list:
                        if t.lower() not in RELEVANT_TAGS:
                            new_tags.append(t)
                    if len(new_tags) != len(current_tags_list):
                        print(f"  -> Lager=0 => tar bort relevanta taggar => {new_tags}")
                        update_product_tags(p_id, new_tags)
                    else:
                        print("  -> Inga relevanta taggar att ta bort.")

                    # => Ta bort series
                    #   Här antar vi att vi vill rensa "product_type" helt från dessa
                    #   Om product_type= "men,unisex" => new_series= [] om 0
                    print("  -> Lager=0 => tar bort serier.")
                    update_product_series(p_id, [])
                else:
                    # => LÄGG TILLBAKA relevanta taggar från DB
                    new_tags = current_tags_list[:]
                    changed = False
                    for rt in original_db_tags:
                        if rt not in new_tags:
                            new_tags.append(rt)
                            changed = True
                    if changed:
                        print(f"  -> Lager>0 => lägger tillbaka taggar: {original_db_tags} => {new_tags}")
                        update_product_tags(p_id, new_tags)
                    else:
                        print("  -> Lager>0 => inga relevanta taggar saknades.")

                    # => LÄGG TILLBAKA serie-lista
                    if current_series_list:
                        print(f"  -> Lager>0 => lägger tillbaka serier: {current_series_list}")
                        update_product_series(p_id, current_series_list)
                    else:
                        print("  -> Lager>0 => inga relevanta serier.")
            else:
                print(f"  -> Ingen produkt för parfnum={num_float}.\n")

        print("\n[main] KLART – Scriptet har behandlat alla rader i Google Sheet.\n")

    except Exception as e:
        print(f"Fel i main(): {e}")

##############################################################################
#                               STARTPUNKT                                  #
##############################################################################

if __name__ == "__main__":
    main()

