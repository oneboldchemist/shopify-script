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
#                   KONSTANTER OCH GEMENSAMMA FUNKTIONER                     #
##############################################################################

RELEVANT_TAGS = {"male","female","unisex","best seller","bestseller"}
SERIES_MAPPING = {
    "male": "men",
    "female": "women",
    "unisex": "unisex",
    "best seller": "bestsellers",
    "bestseller": "bestsellers"
}

def safe_api_call(func, *args, **kwargs):
    try:
        resp = func(*args, **kwargs)
        time.sleep(1)
        return resp
    except requests.exceptions.RequestException as e:
        print("[safe_api_call] Nätverksfel:", e)
        time.sleep(5)
        return safe_api_call(func, *args, **kwargs)

def skip_product_title(title: str) -> bool:
    lower_t = title.lower()
    return ("sample" in lower_t or "bundle" in lower_t)

def extract_perfume_number_from_product_title(title: str):
    pattern = r"\b(\d{1,3}(\.\d+)?)(?!\s*\d)"
    match = re.search(pattern, title.lower())
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def normalize_minus_sign(value_str):
    if not value_str:
        return value_str
    return (value_str
            .replace('−','-')
            .replace('\u2212','-'))

def build_series_list(tag_list):
    sset = set()
    for t in tag_list:
        l = t.lower()
        if l in SERIES_MAPPING:
            sset.add(SERIES_MAPPING[l])
    return sorted(sset)

##############################################################################
#                DB-FUNKTION: relevant_tags_cache (product_id->tags)         #
##############################################################################

def load_tags_cache(db_url):
    """
    Laddar en tabell med:
      product_id TEXT PRIMARY KEY,
      tags TEXT NOT NULL
    Returnerar ex: { "8859929837910": ["BEST SELLER","Male"], ... }
    (endast Store1s product_id)
    """
    conn = psycopg2.connect(db_url)
    data = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT product_id, tags FROM relevant_tags_cache;")
        rows = cur.fetchall()
        for row in rows:
            pid = row["product_id"]
            tstr= row["tags"] or ""
            tlist= tstr.split(",") if tstr else []
            data[pid] = tlist
    conn.close()
    return data

##############################################################################
#               FETCH ALL PRODUCTS FRÅN EN STORE => ID -> product            #
##############################################################################

def fetch_products_id_map(domain, token):
    """
    Return => { str(product_id): product_dict }
    skip sample/bundle
    """
    base_url = f"https://{domain}/admin/api/2023-07"
    endpoint = base_url + "/products.json"
    headers = {"X-Shopify-Access-Token": token}
    params= {"limit":250}
    out = {}
    while True:
        r = safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if r.status_code==200:
            d=r.json()
            prods=d.get("products",[])
            for p in prods:
                pid = str(p["id"])
                title = p.get("title","")
                if skip_product_title(title):
                    continue
                out[pid] = p
            link_h= r.headers.get("Link","")
            next_link=None
            if 'rel="next"' in link_h:
                for part in link_h.split(','):
                    if 'rel="next"' in part:
                        next_link= part[part.find("<")+1:part.find(">")]
                        break
            if next_link:
                endpoint= next_link
                params={}
            else:
                break
        else:
            print(f"[fetch_products_id_map] FEL {r.status_code}: {r.text}")
            break
    return out

##############################################################################
#    FETCH ALL PRODUCTS FRÅN STORE2 => TITLE -> product_dict (skip sample)   #
##############################################################################

def fetch_products_by_title_map(domain, token):
    """
    Return => { title.lower(): product_dict }
    skip sample/bundle
    """
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/products.json"
    headers= {"X-Shopify-Access-Token": token}
    params={"limit":250}
    store_map={}
    while True:
        r= safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if r.status_code==200:
            dd=r.json()
            prods= dd.get("products",[])
            for p in prods:
                t= p.get("title","")
                if skip_product_title(t):
                    continue
                store_map[t.lower()] = p
            link_h= r.headers.get("Link","")
            next_link=None
            if 'rel="next"' in link_h:
                for part in link_h.split(','):
                    if 'rel="next"' in part:
                        next_link= part[part.find("<")+1:part.find(">")]
                        break
            if next_link:
                endpoint= next_link
                params={}
            else:
                break
        else:
            print(f"[fetch_products_by_title_map] FEL {r.status_code}: {r.text}")
            break
    return store_map

##############################################################################
#       UPPDATERA LAGER & TAGGAR & KOLLEKTIONER I STORE (gemensamma)         #
##############################################################################

def update_inventory_level(domain, token, location_id, inventory_item_id, qty):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/inventory_levels/set.json"
    headers= {
        "X-Shopify-Access-Token": token,
        "Content-Type":"application/json"
    }
    payload= {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": qty
    }
    r= safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if r.status_code==200:
        print(f"      => OK, lager => {qty}")
    else:
        print(f"      => FEL {r.status_code}: {r.text}")

def update_product_tags(domain, token, product_id, new_tags):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + f"/products/{product_id}.json"
    headers={
        "X-Shopify-Access-Token": token,
        "Content-Type":"application/json"
    }
    payload={
        "product": {
            "id": product_id,
            "tags": ",".join(new_tags)
        }
    }
    r= safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if r.status_code==200:
        print(f"      => OK, taggar => {new_tags}")
    else:
        print(f"      => FEL {r.status_code}: {r.text}")

def get_collections_for_product(domain, token, product_id):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/collects.json"
    headers= {"X-Shopify-Access-Token": token}
    params= {"product_id":product_id,"limit":250}
    c_map={}
    while True:
        r= safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if r.status_code==200:
            dd=r.json()
            collects= dd.get("collects",[])
            for c in collects:
                cid= c["collection_id"]
                c_map[cid]= c["id"]
            link_h= r.headers.get("Link","")
            next_link=None
            if 'rel="next"' in link_h:
                for part in link_h.split(','):
                    if 'rel="next"' in part:
                        next_link= part[part.find("<")+1:part.find(">")]
                        break
            if next_link:
                endpoint= next_link
                params={}
            else:
                break
        else:
            print(f"[get_collections_for_product] FEL {r.status_code}: {r.text}")
            break
    return c_map

def add_product_to_collection(domain, token, product_id, collection_id):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/collects.json"
    headers= {
        "X-Shopify-Access-Token": token,
        "Content-Type":"application/json"
    }
    payload= {
        "collect":{
            "product_id": product_id,
            "collection_id": collection_id
        }
    }
    r= safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if r.status_code==201:
        print(f"      => OK, lade till product {product_id} i kollektion {collection_id}")
    else:
        print(f"      => FEL {r.status_code}: {r.text}")

def remove_product_from_collection(domain, token, collect_id):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + f"/collects/{collect_id}.json"
    headers={"X-Shopify-Access-Token": token}
    r= safe_api_call(requests.delete, endpoint, headers=headers)
    if r.status_code==200:
        print(f"      => OK, tog bort collect {collect_id}")
    else:
        print(f"      => FEL {r.status_code}: {r.text}")

def update_collections_for_product(domain, token, product_id, wanted_series, coll_map):
    """
    coll_map: ex. { "men": 633426805078, "women":..., "unisex":..., "bestsellers":... }
    """
    existing_map= get_collections_for_product(domain, token, product_id)
    wanted_ids=set()
    for s in wanted_series:
        if s in coll_map:
            wanted_ids.add(coll_map[s])
    existing_ids=set(existing_map.keys())
    add_ids= wanted_ids- existing_ids
    remove_ids= existing_ids- wanted_ids

    if add_ids:
        print(f"   -> add {list(add_ids)}")
        for cid in add_ids:
            add_product_to_collection(domain, token, product_id, cid)
    else:
        print("   -> inga nya kollektioner att lägga till")

    if remove_ids:
        print(f"   -> remove {list(remove_ids)}")
        for cid in remove_ids:
            col_id = existing_map[cid]
            remove_product_from_collection(domain, token, col_id)
    else:
        print("   -> inga kollektioner att ta bort")

##############################################################################
#     STEG 1: UPPDATERA STORE1 DIREKT (product_id = DB)                      #
##############################################################################

def process_store1(db_tags, store1_domain, store1_token, store1_location, store1_collmap, records):
    """
    1) Hämta ALLA products i Store1 => id->product
    2) loop => om id i DB => hämta tags => extrahera parfymnr => google-lager => sätt lager =>taggar => kollektion
    """
    print("\n--- process_store1 (master) ---\n")
    store1_map= fetch_products_id_map(store1_domain, store1_token)

    # Bygg en map parfnum => antal (från Google-lager)
    parfnum_map={}
    for row in records:
        raw_n= normalize_minus_sign(str(row.get("nummer:","")))
        raw_a= normalize_minus_sign(str(row.get("Antal:","")))
        if not raw_n or not raw_a:
            continue
        try:
            n_f= float(raw_n)
            a_i= int(raw_a)
            if a_i<0: a_i=0
            parfnum_map[n_f]= a_i
        except ValueError:
            pass

    # Loop store1_map
    for pid, product_data in store1_map.items():
        # Finns pid i db_tags?
        if pid not in db_tags:
            # ej i DB => skip
            continue
        # extrahera parfnum ur productens "title"
        title= product_data.get("title","")
        parfnum= extract_perfume_number_from_product_title(title)
        if parfnum is None:
            continue
        if parfnum not in parfnum_map:
            print(f"  => Ingen lagerinfo för parfnum={parfnum} i google-lager (title='{title}')")
            continue
        qty= parfnum_map[parfnum]
        # hämta tags i DB
        taglist= db_tags[pid]
        series_list= build_series_list(taglist)
        # nuvarande shopify tags
        shop_tags_str= product_data.get("tags","")
        shop_tags_list= [t.strip() for t in shop_tags_str.split(",") if t.strip()]

        # => uppdatera lager
        variants= product_data.get("variants",[])
        for var in variants:
            inv_id= var.get("inventory_item_id")
            if inv_id:
                update_inventory_level(store1_domain, store1_token, store1_location, inv_id, qty)

        if qty==0:
            # ta bort relevanta taggar
            new_t=[]
            for t in shop_tags_list:
                if t.lower() not in RELEVANT_TAGS:
                    new_t.append(t)
            if len(new_t)!= len(shop_tags_list):
                update_product_tags(store1_domain, store1_token, pid, new_t)
            # ta bort kollektion
            update_collections_for_product(store1_domain, store1_token, pid, [], store1_collmap)
        else:
            # lägg tillbaka
            changed=False
            new_tags= shop_tags_list[:]
            for rt in taglist:
                if rt not in new_tags:
                    new_tags.append(rt)
                    changed=True
            if changed:
                update_product_tags(store1_domain, store1_token, pid, new_tags)
            if series_list:
                update_collections_for_product(store1_domain, store1_token, pid, series_list, store1_collmap)
            else:
                update_collections_for_product(store1_domain, store1_token, pid, [], store1_collmap)

##############################################################################
#     STEG 2: UPPDATERA STORE2 GENOM MATCHA "title" FRÅN STORE1-PRODUKT      #
##############################################################################

def process_store2(db_tags, store1_domain, store1_token, store2_domain, store2_token, store2_location, store2_collmap, records):
    """
    1) Hämtar Store1-produkter => ID->product => skip sample => t.ex. "8859929837910" -> {title,variants,...}
    2) Hämtar store2 => title.lower() -> product
    3) DB => product_id => tags
       => i store1 => hämta product => get title => extrahera parfnum => google-lager => i store2 => find product => uppd
    """
    print("\n--- process_store2 (översätter store1->title->store2) ---\n")

    # Bygg parfnum-lager (google-lager)
    parfnum_map={}
    for row in records:
        raw_n= normalize_minus_sign(str(row.get("nummer:","")))
        raw_a= normalize_minus_sign(str(row.get("Antal:","")))
        if not raw_n or not raw_a:
            continue
        try:
            n_f= float(raw_n)
            a_i= int(raw_a)
            if a_i<0: a_i=0
            parfnum_map[n_f]= a_i
        except ValueError:
            pass

    # store1: id->product
    store1_id_map= fetch_products_id_map(store1_domain, store1_token)
    # store2: title.lower()-> product
    store2_title_map= fetch_products_by_title_map(store2_domain, store2_token)

    # loop DB => product_id => tags
    for pid, taglist in db_tags.items():
        # hämta store1-product => "title"
        if pid not in store1_id_map:
            # DB har product_id men store1 API gav ingen match => skip
            continue
        product_data= store1_id_map[pid]
        title= product_data.get("title","")
        if skip_product_title(title):
            continue
        parfnum= extract_perfume_number_from_product_title(title)
        if parfnum is None:
            continue
        # hämta lager
        if parfnum not in parfnum_map:
            print(f"  => Ingen lagerdata för parfnum={parfnum} i google-lager, skip (title='{title}')")
            continue
        qty= parfnum_map[parfnum]

        # i store2 => matchar title.lower()
        store2_product= store2_title_map.get(title.lower())
        if not store2_product:
            print(f"  => Hittar ej '{title}' i store2")
            continue
        store2_pid= str(store2_product["id"])
        variants= store2_product.get("variants",[])

        # => sätt lager
        for var in variants:
            inv_id= var.get("inventory_item_id")
            if inv_id:
                update_inventory_level(store2_domain, store2_token, store2_location, inv_id, qty)

        # => build series
        series_list= build_series_list(taglist)
        # => shopify tags store2
        shop_tags_str= store2_product.get("tags","")
        shop_tags_list= [t.strip() for t in shop_tags_str.split(",") if t.strip()]

        if qty==0:
            # ta bort relevanta
            new_t=[]
            for t in shop_tags_list:
                if t.lower() not in RELEVANT_TAGS:
                    new_t.append(t)
            if len(new_t)!= len(shop_tags_list):
                update_product_tags(store2_domain, store2_token, store2_pid, new_t)
            # ta bort kollektioner
            update_collections_for_product(store2_domain, store2_token, store2_pid, [], store2_collmap)
        else:
            # lägg tillbaka
            changed=False
            new_t= shop_tags_list[:]
            for rt in taglist:
                if rt not in new_t:
                    new_t.append(rt)
                    changed=True
            if changed:
                update_product_tags(store2_domain, store2_token, store2_pid, new_t)
            if series_list:
                update_collections_for_product(store2_domain, store2_token, store2_pid, series_list, store2_collmap)
            else:
                update_collections_for_product(store2_domain, store2_token, store2_pid, [], store2_collmap)

##############################################################################
#                                   MAIN                                     #
##############################################################################

def main():
    try:
        # 1) Hämta env
        DB_URL= os.getenv("DATABASE_URL")  
        if not DB_URL:
            raise ValueError("Saknas env var: DATABASE_URL")

        # Store1
        s1_domain= os.getenv("STORE1_DOMAIN")
        s1_token= os.getenv("STORE1_TOKEN")
        s1_loc= os.getenv("STORE1_LOCATION_ID")
        s1_men= int(os.getenv("STORE1_MEN_COLLECTION_ID","0"))
        s1_women= int(os.getenv("STORE1_WOMEN_COLLECTION_ID","0"))
        s1_uni= int(os.getenv("STORE1_UNISEX_COLLECTION_ID","0"))
        s1_best= int(os.getenv("STORE1_BESTSELLERS_COLLECTION_ID","0"))

        # Store2
        s2_domain= os.getenv("STORE2_DOMAIN")
        s2_token= os.getenv("STORE2_TOKEN")
        s2_loc= os.getenv("STORE2_LOCATION_ID")
        s2_men= int(os.getenv("STORE2_MEN_COLLECTION_ID","0"))
        s2_women= int(os.getenv("STORE2_WOMEN_COLLECTION_ID","0"))
        s2_uni= int(os.getenv("STORE2_UNISEX_COLLECTION_ID","0"))
        s2_best= int(os.getenv("STORE2_BESTSELLERS_COLLECTION_ID","0"))

        # 2) Google-lager
        GCRED= os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not GCRED:
            raise ValueError("Saknas env var: GOOGLE_CREDENTIALS_JSON")
        scope= ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds_dict= json.loads(GCRED)
        google_creds= ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc= gspread.authorize(google_creds)

        sheet= gc.open("OBC lager").sheet1
        records= sheet.get_all_records(expected_headers=["nummer:", "Antal:"])
        print(f"[main] => {len(records)} rader i Google-lager.\n")

        # 3) Ladda DB (relevant_tags_cache => store1 product_id => taglist)
        db_tags= load_tags_cache(DB_URL)

        # 4) Uppdatera Store1 direkt
        print("\n--- STEG A: Uppdatera Store1 direkt med DB product_id ---\n")
        process_store1(
            db_tags,                   # dict product_id->tags
            s1_domain,
            s1_token,
            s1_loc,
            {
                "men": s1_men,
                "women": s1_women,
                "unisex": s1_uni,
                "bestsellers": s1_best
            },
            records
        )

        # 5) Uppdatera Store2 genom att matcha "title" från Store1
        print("\n--- STEG B: Uppdatera Store2 genom att hitta samma titel ---\n")
        process_store2(
            db_tags,                  # Samma DB-läsning
            s1_domain, s1_token,      # för att hämta store1s title
            s2_domain, s2_token,      # store2
            s2_loc,
            {
                "men": s2_men,
                "women": s2_women,
                "unisex": s2_uni,
                "bestsellers": s2_best
            },
            records
        )

        print("\n[main] => KLART! Store1 och Store2 uppdaterade.\n")

    except Exception as e:
        print(f"Fel i main(): {e}")

if __name__=="__main__":
    main()
