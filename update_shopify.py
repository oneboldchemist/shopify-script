import os
import re
import time
import json
import requests
import gspread

from oauth2client.service_account import ServiceAccountCredentials

##############################################################################
#                         KONFIGURATION & VARIABLER                          #
##############################################################################

# === Shopify-inställningar via Environment Variables ===
SHOP_DOMAIN = os.getenv("SHOP_DOMAIN", "your-shop-domain.myshopify.com")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "your-access-token")
LOCATION_ID  = os.getenv("LOCATION_ID", "1234567890")

# Bygg bas-URL för Shopify
BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/2023-07"

# === Google Sheets-inställningar via Environment Variable ===
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDS_JSON:
    raise ValueError("Saknar environment var: GOOGLE_CREDENTIALS_JSON (Google Service Account JSON)")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(GOOGLE_CREDS_JSON)
google_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(google_creds)

# De relevanta taggarna
RELEVANT_TAGS = {"male", "female", "unisex", "BESTSELLER", "BEST SELLER"}

# Fil för att lagra "cache" över relevanta taggar
TAGS_CACHE_FILE = "tags_cache.json"

##############################################################################
#                           HJÄLPFUNKTIONER                                  #
##############################################################################

def safe_api_call(func, *args, **kwargs):
    """
    Enkel wrapper för att anropa Shopify/Google API med kort paus
    för att undvika rate limit.
    """
    try:
        response = func(*args, **kwargs)
        time.sleep(1)
        return response
    except requests.exceptions.RequestException as e:
        print("[safe_api_call] Nätverksfel:", e)
        time.sleep(5)
        return safe_api_call(func, *args, **kwargs)

def normalize_minus_sign(value_str: str):
    """
    Ersätter ev. långa minus-tecken (ex. U+2212) med vanligt “-” (U+002D).
    Ex: “−12” -> “-12”.
    """
    if not value_str:
        return value_str
    return (value_str
            .replace('−', '-')
            .replace('\u2212', '-'))

def extract_perfume_number_from_product_title(product_title: str):
    """
    Letar efter 1-3 siffror + ev decimal (ex. 149, 149.0, 22.5),
    men ignorerar om fler siffror följer (ex. “1 000,00 kr” -> ej 1.0).
    """
    pattern = r"\b(\d{1,3}(\.\d+)?)(?!\s*\d)"
    match = re.search(pattern, product_title)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def update_inventory_level(inventory_item_id, new_quantity):
    """
    Sätter lagersaldo i Shopify (inventory_levels/set).
    """
    endpoint = f"{BASE_URL}/inventory_levels/set.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "location_id": LOCATION_ID,
        "inventory_item_id": inventory_item_id,
        "available": new_quantity
    }

    print(f"   [update_inventory_level] → {endpoint}")
    print(f"       location_id={LOCATION_ID}, inventory_item_id={inventory_item_id}, available={new_quantity}")

    response = safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! Lagersaldo satt till {new_quantity}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

def update_product_tags(product_id, new_tag_list):
    """
    Uppdaterar en produkts 'tags' via PUT /products/{id}.json.
    """
    tags_str = ",".join(new_tag_list)
    endpoint = f"{BASE_URL}/products/{product_id}.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "product": {
            "id": product_id,
            "tags": tags_str
        }
    }

    print(f"   [update_product_tags] → PUT {endpoint}")
    print(f"       product_id={product_id}, tags='{tags_str}'")

    response = safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"   -> OK! Taggar uppdaterade: {new_tag_list}\n")
    else:
        print(f"   -> FEL! Status {response.status_code}: {response.text}\n")

##############################################################################
#                LADDA & SPARA CACHE ÖVER RELEVANTA TAGGAR                   #
##############################################################################

def load_tags_cache():
    """
    Läser in en JSON-fil (tags_cache.json) -> dict { product_id: [“male”, “BESTSELLER” ... ] }
    om filen ej finns -> {}
    """
    if not os.path.isfile(TAGS_CACHE_FILE):
        return {}
    try:
        with open(TAGS_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            else:
                return {}
    except Exception as e:
        print(f"[load_tags_cache] FEL: {e}")
        return {}

def save_tags_cache(tags_dict):
    """
    Sparar cachen till tags_cache.json.
    """
    try:
        with open(TAGS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(tags_dict, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[save_tags_cache] FEL: {e}")

##############################################################################
#                    HÄMTA ALLA PRODUKTER & SKIPPA "SAMPLE"                  #
##############################################################################

def fetch_all_products():
    """
    Hämtar alla produkter via /products.json (paginering).
    Returnerar list[dict].
    """
    print("[fetch_all_products] Hämtar alla produkter från Shopify...")
    all_products = []
    endpoint = f"{BASE_URL}/products.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN
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
    Körs som standard. 
    1) Ladda cache av relevanta taggar.
    2) Hämta alla produkter, skippar "sample", extrahera parfymnummer.
    3) Bygg perfume_lookup, spara ev. relevanta taggar i cache.
    4) Öppna Google Sheet “OBC lager” & läs rader (nummer:, Antal:).
    5) Sätt lager i Shopify + hantera relevanta taggar.
    """
    try:
        # 1) Ladda cachen
        relevant_tags_cache = load_tags_cache()

        # 2) Hämta produkter
        all_products = fetch_all_products()

        perfume_lookup = {}
        print("[main] Bygger 'perfume_lookup'...\n")
        for i, product in enumerate(all_products, start=1):
            product_id = product["id"]
            title = product.get("title", "")
            tags_str = product.get("tags", "")  # ex. "male,female,BEST SELLER"
            if "sample" in title.lower():
                print(f"  [Produkt #{i}] \"{title}\" -> innehåller 'sample', skippar.")
                continue

            # extrahera parfymnummer
            num = extract_perfume_number_from_product_title(title)

            # om product_id ej i cachen -> spara enbart de relevanta taggarna
            pid_str = str(product_id)
            if pid_str not in relevant_tags_cache:
                current_tags = [t.strip() for t in tags_str.split(",") if t.strip()]
                relevant_found = []
                for t in current_tags:
                    if t.lower() in [rt.lower() for rt in RELEVANT_TAGS]:
                        relevant_found.append(t)
                relevant_tags_cache[pid_str] = relevant_found

            if num is not None:
                perfume_lookup[num] = product
                print(f"  [Produkt #{i}] \"{title}\" (id={product_id}) -> parfymnr={num}, tags={tags_str}")
            else:
                print(f"  [Produkt #{i}] \"{title}\" -> Ingen parfymnr-match.")

        # 3) Öppna Google Sheet “OBC lager”
        print("\n[main] Öppnar Google Sheet 'OBC lager'...")
        sheet = client.open("OBC lager").sheet1
        rows = sheet.get_all_records()
        print(f"[main] Antal rader: {len(rows)}\n")

        # 4) Loopa rader
        for idx, row in enumerate(rows, start=1):
            raw_num = str(row.get("nummer:", "")).strip()
            raw_antal = str(row.get("Antal:", "")).strip()

            print(f"--- [Rad #{idx}] ---------------------------------------------------")
            print(f"  -> nummer: {raw_num}, Antal: {raw_antal}")

            if not raw_num or not raw_antal:
                print("  -> Ogiltig rad. Hoppar över.\n")
                continue

            raw_num = normalize_minus_sign(raw_num)
            raw_antal = normalize_minus_sign(raw_antal)

            try:
                num_float = float(raw_num)
                antal_int = int(raw_antal)
                if antal_int < 0:
                    print(f"  -> Antalet är negativt ({antal_int}). Sätter 0.")
                    antal_int = 0
            except ValueError:
                print("  -> Kan ej tolka nummer/Antal som siffror. Hoppar över.\n")
                continue

            # Finns parfymnummer i perfume_lookup?
            if num_float in perfume_lookup:
                product_data = perfume_lookup[num_float]
                product_id = product_data["id"]
                product_tags_str = product_data.get("tags", "")
                product_variants = product_data.get("variants", [])
                pid_str = str(product_id)

                print(f"  -> MATCH: Parfymnr {num_float}, product_id={product_id}, sätter lager={antal_int}")
                # uppdatera varje variant
                for variant in product_variants:
                    inv_item_id = variant.get("inventory_item_id")
                    if inv_item_id:
                        update_inventory_level(inv_item_id, antal_int)

                # Nu hanterar vi relevanta taggar:
                current_tags_list = [t.strip() for t in product_tags_str.split(",") if t.strip()]
                original_relevant_tags = relevant_tags_cache.get(pid_str, [])

                if antal_int == 0:
                    # Ta bort enbart relevanta taggar
                    new_tags = []
                    for t in current_tags_list:
                        if t.lower() not in [r.lower() for r in RELEVANT_TAGS]:
                            new_tags.append(t)
                    if len(new_tags) != len(current_tags_list):
                        print(f"  -> Lager=0. Tar bort relevanta taggar. Nya tags: {new_tags}")
                        update_product_tags(product_id, new_tags)
                    else:
                        print("  -> Inga relevanta taggar att ta bort.")
                else:
                    # Lager > 0 => lägg tillbaks ursprungliga relevanta (om saknas)
                    new_tags = current_tags_list[:]
                    changed = False
                    for rt in original_relevant_tags:
                        if rt not in new_tags:
                            new_tags.append(rt)
                            changed = True
                    if changed:
                        print(f"  -> Lager>0. Lägger tillbaka relevanta taggar: {original_relevant_tags}.")
                        update_product_tags(product_id, new_tags)
                    else:
                        print("  -> Lager>0. Inga relevanta taggar saknades.")
            else:
                print(f"  -> Ingen matchande produkt för parfymnr={num_float}.\n")

        # 5) Spara cachen
        print("[main] Sparar relevant_tags_cache...")
        save_tags_cache(relevant_tags_cache)

        print("\n[main] KLART – Scriptet har behandlat alla rader.\n")

    except Exception as e:
        print(f"Fel i main(): {e}")

if __name__ == "__main__":
    main()

