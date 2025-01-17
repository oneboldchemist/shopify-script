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
#                    GEMENSAMMA KONSTANTER OCH FUNKTIONER                    #
##############################################################################

# Nedan har vi ENBART "bestseller" som giltig variant, ej "best seller"  # <-- [NYTT/ÄNDRAT]
RELEVANT_TAGS = {"male","female","unisex","bestseller"}

# SERIES_MAPPING har också enbart "bestseller"  # <-- [NYTT/ÄNDRAT]
SERIES_MAPPING = {
    "male": "men",
    "female": "women",
    "unisex": "unisex",
    "bestseller": "bestsellers"
}

def safe_api_call(func, *args, **kwargs):
    try:
        r = func(*args, **kwargs)
        time.sleep(1)
        return r
    except requests.exceptions.RequestException as e:
        print("[safe_api_call] Nätverksfel:", e)
        time.sleep(5)
        return safe_api_call(func, *args, **kwargs)

def skip_product_title(title:str)->bool:
    lower_t = title.lower()
    return ("sample" in lower_t or "bundle" in lower_t)

def extract_perfume_number_from_product_title(title:str):
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
    """
    Returnerar en lista med kollektioner baserat på taggar som matchar SERIES_MAPPING.
    T.ex. ['men','women','unisex','bestsellers'] beroende på vilka taggar som finns.
    """
    sset = set()
    for t in tag_list:
        l = t.lower()
        if l in SERIES_MAPPING:
            sset.add(SERIES_MAPPING[l])
    return sorted(sset)

##############################################################################
#       DB-FUNKTION: relevant_tags_cache => (product_id TEXT, tags TEXT)     #
##############################################################################

def load_tags_cache(db_url):
    """
    Hämtar alla (product_id, tags) från tabellen relevant_tags_cache i DB.
    Returnerar en dict { '8859929837910': ['BESTSELLER','Male'], ... }
    """
    conn = psycopg2.connect(db_url)
    from psycopg2.extras import RealDictCursor
    store_dict={}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT product_id, tags FROM relevant_tags_cache;")
        rows=cur.fetchall()
        for row in rows:
            pid= row["product_id"]
            tstr= row["tags"] or ""
            tlist= tstr.split(",") if tstr else []
            # Normalisera (ta bort extra spaces osv)
            clean_tags = [x.strip() for x in tlist if x.strip()]
            store_dict[pid]= clean_tags
    conn.close()
    return store_dict

##############################################################################
#           HÄMTA ALLA PRODUKTER FRÅN EN STORE (id->product)                #
##############################################################################

def fetch_store_id_map(domain, token):
    """
    Return => { product_id (str): product_dict}
    skip sample/bundle
    """
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/products.json"
    headers= {"X-Shopify-Access-Token": token}
    params= {"limit":250}
    out_map={}
    while True:
        r= safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if r.status_code==200:
            data= r.json()
            prods= data.get("products",[])
            for p in prods:
                pid= str(p["id"])
                title= p.get("title","")
                if skip_product_title(title):
                    continue
                out_map[pid]= p
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
            print(f"[fetch_store_id_map] FEL {r.status_code}: {r.text}")
            break
    return out_map

##############################################################################
#      HÄMTA PRODUKTER FRÅN STORE 2 => TITLE.LOWER() => product_dict         #
##############################################################################

def fetch_store_title_map(domain, token):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/products.json"
    headers= {"X-Shopify-Access-Token": token}
    params= {"limit":250}
    title_map={}
    while True:
        r= safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if r.status_code==200:
            dd= r.json()
            prods= dd.get("products",[])
            for p in prods:
                t= p.get("title","")
                if skip_product_title(t):
                    continue
                title_map[t.lower()]= p
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
            print(f"[fetch_store_title_map] FEL {r.status_code}: {r.text}")
            break
    return title_map

##############################################################################
#      INVENTORY, TAGS, KOLLEKTIONER - FUNKTIONER                            #
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
    rr= safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if rr.status_code==200:
        print(f"       => OK, lager satt till {qty}.")
    else:
        print(f"       => FEL {rr.status_code}: {rr.text}")

def update_product_tags(domain, token, product_id, new_tags_list):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + f"/products/{product_id}.json"
    headers={
        "X-Shopify-Access-Token": token,
        "Content-Type":"application/json"
    }
    payload={
        "product":{
            "id": product_id,
            "tags": ",".join(new_tags_list)
        }
    }
    rr= safe_api_call(requests.put, endpoint, headers=headers, json=payload)
    if rr.status_code==200:
        print(f"       => OK, taggar uppdaterade till: {new_tags_list}")
    else:
        print(f"       => FEL {rr.status_code}: {rr.text}")

def get_collections_for_product(domain, token, product_id):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + "/collects.json"
    headers= {"X-Shopify-Access-Token": token}
    params= {"product_id": product_id, "limit":250}
    col_map={}
    while True:
        r= safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if r.status_code==200:
            dd=r.json()
            c_list= dd.get("collects",[])
            for c in c_list:
                cid= c["collection_id"]
                col_map[cid]= c["id"]
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
    return col_map

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
    rr= safe_api_call(requests.post, endpoint, headers=headers, json=payload)
    if rr.status_code==201:
        print(f"       => OK, lade till produkt {product_id} i kollektion {collection_id}")
    else:
        print(f"       => FEL {rr.status_code}: {rr.text}")

def remove_product_from_collection(domain, token, collect_id):
    base_url= f"https://{domain}/admin/api/2023-07"
    endpoint= base_url + f"/collects/{collect_id}.json"
    headers={"X-Shopify-Access-Token": token}
    rr= safe_api_call(requests.delete, endpoint, headers=headers)
    if rr.status_code==200:
        print(f"       => OK, tog bort collect {collect_id}")
    else:
        print(f"       => FEL {rr.status_code}: {rr.text}")

def update_collections_for_product(domain, token, product_id, new_series, col_map):
    """
    Uppdaterar vilka kollektioner (serier) produkten ska ligga i.
    new_series är en lista, t.ex. ['men','unisex'].
    col_map är en dict { 'men': ID, 'women':ID, ...}.
    """
    existing_map= get_collections_for_product(domain, token, product_id)
    wanted_ids=set()
    for s in new_series:
        if s in col_map:
            wanted_ids.add(col_map[s])
    existing_ids=set(existing_map.keys())

    add_ids= wanted_ids - existing_ids
    remove_ids= existing_ids - wanted_ids

    if add_ids:
        print(f"   -> kollektioner att LÄGGA TILL: {list(add_ids)}")
        for cid in add_ids:
            add_product_to_collection(domain, token, product_id, cid)
    else:
        print("   -> inga nya kollektioner att lägga till")

    if remove_ids:
        print(f"   -> kollektioner att TA BORT: {list(remove_ids)}")
        for cid in remove_ids:
            c_id= existing_map[cid]
            remove_product_from_collection(domain, token, c_id)
    else:
        print("   -> inga kollektioner att ta bort")

##############################################################################
#            UPPDATERA STORE 1: DIREKT MATCH product_id => DB                #
##############################################################################

def process_store1(db_tags, domain, token, location_id, coll_map, records):
    """
    1) Bygg parfnum->antal (Google-lager)
    2) Hämta store1_products => id->product
    3) För varje produkt i store1:
       - extrahera parfnum
       - sätt lager
       - uppdatera taggar
       - uppdatera kollektioner
       - logga tydligt vad som händer
    """
    print("\n--- process_store1 ---\n")

    # (1) Bygg parfnum->antal (från Google-lager)
    parfnum_map={}
    for r in records:
        raw_n= normalize_minus_sign(str(r.get("nummer:","")))
        raw_a= normalize_minus_sign(str(r.get("Antal:","")))
        if not raw_n or not raw_a:
            continue
        try:
            nf= float(raw_n)
            ai= int(raw_a)
            if ai<0: ai=0
            parfnum_map[nf]= ai
        except ValueError:
            pass

    # (2) Hämta store1-products (id->product)
    store_map= fetch_store_id_map(domain, token)

    # (3) Gå igenom alla relevanta produkter
    for pid, product_data in store_map.items():
        title= product_data.get("title","")
        parfnum= extract_perfume_number_from_product_title(title)
        if parfnum is None:
            continue  # skip om vi inte kan hitta nr

        # Hämta lager från Google-lager
        qty= parfnum_map.get(parfnum)
        if qty is None:
            print(f"  => Ingen Google-lagerinfo för parfymnr={parfnum} (title='{title}'), skippar.")
            continue

        # Hämta taggar från DB om finns, annars från Shopify
        if pid in db_tags:
            taglist = db_tags[pid][:]
        else:
            # Om ej i DB => utgå ifrån existerande Shopify-taggar
            st = product_data.get("tags","")
            st_list= [t.strip() for t in st.split(",") if t.strip()]
            taglist = st_list
        
        # Hämta även existerande taggar i Shopify
        shopify_existing = product_data.get("tags","")
        shopify_list = [t.strip() for t in shopify_existing.split(",") if t.strip()]

        # Slå ihop DB-taggar och Shopify-taggar (standardisera "best seller" -> "bestseller") # <-- [NYTT/ÄNDRAT]
        combined_tags = []
        for x in (taglist + shopify_list):
            lx = x.lower()
            if lx == "best seller":  
                lx = "bestseller"
            combined_tags.append(lx)
        combined_set = set(combined_tags)
        
        # För debug: spara gamla innan vi ändrar
        old_shopify_tags = list(shopify_list)

        # Sätt lager (inventory)
        print(f"\n** [STORE1] Hanterar produkt: PID={pid}, Titel='{title}', Parfymnr={parfnum}, Lager={qty} **")
        variants= product_data.get("variants",[])
        for var in variants:
            inv_id= var.get("inventory_item_id")
            if inv_id:
                update_inventory_level(domain, token, location_id, inv_id, qty)

        # Beroende på om qty=0 ska vi ev ta bort relevanta taggar/kollektioner
        if qty==0:
            # Ta bort relevanta taggar i RELEVANT_TAGS
            new_t = []
            for t in combined_set:
                if t not in RELEVANT_TAGS:
                    new_t.append(t)
            new_t = sorted(list(set(new_t)))  # unik & sorterad
            print(f"   Gamla Shopify-taggar: {old_shopify_tags}")
            print(f"   Nya Shopify-taggar (efter borttagning): {new_t}")
            update_product_tags(domain, token, pid, new_t)

            print("   => Tar bort samtliga kollektioner (eftersom qty=0).")
            update_collections_for_product(domain, token, pid, [], coll_map)

        else:
            # Lägg till relevanta taggar om de saknas
            new_t = sorted(list(combined_set))
            print(f"   Gamla Shopify-taggar: {old_shopify_tags}")
            print(f"   Nya Shopify-taggar (efter merge): {new_t}")
            update_product_tags(domain, token, pid, new_t)

            # Bygg ny kollektionslista
            series_list= build_series_list(new_t)  
            print(f"   => Vill uppdatera kollektioner till: {series_list}")
            update_collections_for_product(domain, token, pid, series_list, coll_map)

##############################################################################
#         UPPDATERA STORE 2: “översätt” via Store 1 “title” => Store 2       #
##############################################################################

def process_store2(db_tags,
                   store1_domain, store1_token,
                   store2_domain, store2_token, store2_location,
                   store2_coll_map, records):
    """
    1) Bygg parfnum->antal (Google-lager)
    2) Hämta store1 => id->product => skip sample => ger title
    3) Hämta store2 => title.lower()->product
    4) loopa igenom products i store1, matcha parfnum => db_tags => uppdatera store2
    """
    print("\n--- process_store2 (översätt via title) ---\n")

    # (A) Bygg parfnum-lager (från Google-lager)
    parfnum_map={}
    for r in records:
        raw_n= normalize_minus_sign(str(r.get("nummer:","")))
        raw_a= normalize_minus_sign(str(r.get("Antal:","")))
        if not raw_n or not raw_a:
            continue
        try:
            nf= float(raw_n)
            ai= int(raw_a)
            if ai<0: ai=0
            parfnum_map[nf]= ai
        except ValueError:
            pass

    # (B) store1_id->product
    store1_id_map= fetch_store_id_map(store1_domain, store1_token)
    # (C) store2_title->product
    store2_title_map= fetch_store_title_map(store2_domain, store2_token)

    # (D) Loopa store1-produkter och kolla db_tags => uppdatera store2
    for pid, product_data in store1_id_map.items():
        title= product_data.get("title","")
        parfnum= extract_perfume_number_from_product_title(title)
        if parfnum is None:
            continue
        if parfnum not in parfnum_map:
            print(f"  => Ingen lagerinfo för parfymnr={parfnum} i Google-lager (title='{title}'), skippar.")
            continue
        qty= parfnum_map[parfnum]

        # Hämta taggar för store1-produkten från DB eller Shopify
        if pid in db_tags:
            taglist = db_tags[pid][:]
        else:
            st = product_data.get("tags","")
            st_list = [t.strip() for t in st.split(",") if t.strip()]
            taglist = st_list

        # Hitta motsvarande produkt i store2 via title.lower()
        s2_product= store2_title_map.get(title.lower())
        if not s2_product:
            print(f"  => Hittar ingen match i store2 för title='{title}'")
            continue

        s2_pid= str(s2_product["id"])
        variants= s2_product.get("variants",[])

        print(f"\n** [STORE2] Hanterar produkt: Titel='{title}', Parfymnr={parfnum}, PID={s2_pid}, Lager={qty} **")

        # Sätt lager i store2
        for var in variants:
            inv_id= var.get("inventory_item_id")
            if inv_id:
                update_inventory_level(store2_domain, store2_token, store2_location, inv_id, qty)

        # Kombinera DB-taggar med befintliga Shopify-taggar (store2), standardisera "best seller" -> "bestseller" 
        s2_existing = s2_product.get("tags","")
        s2_list = [t.strip() for t in s2_existing.split(",") if t.strip()]
        
        combined_tags = []
        for x in (taglist + s2_list):
            lx = x.lower()
            if lx == "best seller":
                lx = "bestseller"
            combined_tags.append(lx)
        combined_set = set(combined_tags)

        old_store2_tags = list(s2_list)

        if qty==0:
            # ta bort relevanta
            new_t=[]
            for t in combined_set:
                if t not in RELEVANT_TAGS:
                    new_t.append(t)
            new_t = sorted(list(set(new_t)))
            print(f"   Gamla Shopify-taggar i store2: {old_store2_tags}")
            print(f"   Nya Shopify-taggar (efter borttagning): {new_t}")
            update_product_tags(store2_domain, store2_token, s2_pid, new_t)

            print("   => Tar bort samtliga kollektioner i store2 (qty=0).")
            update_collections_for_product(store2_domain, store2_token, s2_pid, [], store2_coll_map)

        else:
            new_t = sorted(list(combined_set))
            print(f"   Gamla Shopify-taggar i store2: {old_store2_tags}")
            print(f"   Nya Shopify-taggar (efter merge): {new_t}")
            update_product_tags(store2_domain, store2_token, s2_pid, new_t)

            series_list= build_series_list(new_t)
            print(f"   => Vill uppdatera kollektioner i store2 till: {series_list}")
            update_collections_for_product(store2_domain, store2_token, s2_pid, series_list, store2_coll_map)

##############################################################################
#                                   MAIN                                     #
##############################################################################

def main():
    try:
        # 1) Miljövariabler
        db_url= os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("Saknas DATABASE_URL")
        # Store 1
        s1_domain= os.getenv("STORE1_DOMAIN")
        s1_token= os.getenv("STORE1_TOKEN")
        s1_loc= os.getenv("STORE1_LOCATION_ID")
        s1_men= int(os.getenv("STORE1_MEN_COLLECTION_ID","0"))
        s1_women= int(os.getenv("STORE1_WOMEN_COLLECTION_ID","0"))
        s1_uni= int(os.getenv("STORE1_UNISEX_COLLECTION_ID","0"))
        s1_best= int(os.getenv("STORE1_BESTSELLERS_COLLECTION_ID","0"))

        # Store 2
        s2_domain= os.getenv("STORE2_DOMAIN")
        s2_token= os.getenv("STORE2_TOKEN")
        s2_loc= os.getenv("STORE2_LOCATION_ID")
        s2_men= int(os.getenv("STORE2_MEN_COLLECTION_ID","0"))
        s2_women= int(os.getenv("STORE2_WOMEN_COLLECTION_ID","0"))
        s2_uni= int(os.getenv("STORE2_UNISEX_COLLECTION_ID","0"))
        s2_best= int(os.getenv("STORE2_BESTSELLERS_COLLECTION_ID","0"))

        # 2) Google-lager
        gc_json= os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not gc_json:
            raise ValueError("Saknas GOOGLE_CREDENTIALS_JSON")
        scope= ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds_dict= json.loads(gc_json)
        google_creds= ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gs= gspread.authorize(google_creds)

        sheet= gs.open("OBC lager").sheet1
        records= sheet.get_all_records(expected_headers=["nummer:", "Antal:"])
        print(f"[main] => {len(records)} rader i Google-lager.\n")

        # 3) Ladda DB (relevant_tags_cache)
        db_tags= load_tags_cache(db_url)

        # 4) Uppdatera Store1 direkt (master)
        print("\n--- [UPPDATERA STORE 1] ---\n")
        process_store1(
            db_tags,
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
        print("\n--- [UPPDATERA STORE 2] ---\n")
        process_store2(
            db_tags,
            s1_domain, s1_token,    # vi hämtar store1-produkt => "title"
            s2_domain, s2_token,    # uppdaterar store2
            s2_loc,
            {
                "men": s2_men,
                "women": s2_women,
                "unisex": s2_uni,
                "bestsellers": s2_best
            },
            records
        )

        print("\n[main] => KLART! Båda butiker uppdaterade.\n")

    except Exception as e:
        print(f"Fel i main(): {e}")

if __name__=="__main__":
    main()
