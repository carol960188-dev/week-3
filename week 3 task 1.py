import urllib.request
import json
import re
import csv
import sys
import ssl
import os
import traceback


VERIFY_SSL = True  
OUT_HOTELS = "hotels.csv"
OUT_DISTRICTS = "districts.csv"


URL_ZH = "https://resources-wehelp-taiwan-b986132eca78c0b5eeb736fc03240c2ff8b7116.gitlab.io/hotels-ch"
URL_EN = "https://resources-wehelp-taiwan-b986132eca78c0b5eeb736fc03240c2ff8b7116.gitlab.io/hotels-en"


def fetch_text(url: str) -> str:
    if VERIFY_SSL:
        with urllib.request.urlopen(url) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace")
    else:
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(url, context=context) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace")


def parse_payload_to_list(payload: str):
    try:
        data = json.loads(payload)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("data", "result", "items"):
                if k in data and isinstance(data[k], list):
                    return data[k]
    except json.JSONDecodeError:
        pass

    m = re.search(r"(\[\s*{.*?}\s*\])", payload, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    m = re.search(r'"data"\s*:\s*(\[\s*{.*?}\s*\])', payload, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    raise ValueError

def peek_sample(tag, data):
    print(f"=== Peek {tag} ===", flush=True)
    print("type:", type(data), "len:", (len(data) if hasattr(data, "__len__") else "N/A"), flush=True)
    if isinstance(data, list) and data:
        print("First item type:", type(data[0]), flush=True)
        if isinstance(data[0], dict):
            keys = list(data[0].keys())
            print("First item keys:", keys[:20], flush=True)
            for k in keys[:5]:
                sv = str(data[0][k])
                print(f"  {k} => {sv[:100]}{'...' if len(sv)>100 else ''}", flush=True)
        else:
            print("First item preview:", str(data[0])[:200], flush=True)
    print("===============", flush=True)

def norm_record(rec: dict, lang: str) -> dict:
    import re

    if not isinstance(rec, dict):
        return {
            "id": None,
            f"name_{lang}": None,
            f"address_{lang}": None,
            "district": None,
            "rooms": None,
            "phone": None,
            "_raw_name": None
        }

    lower_map = { (k.lower() if isinstance(k, str) else k): k for k in rec.keys() }

    def get_any(*aliases):
        for a in aliases:
            lk = a.lower()
            if lk in lower_map:
                k = lower_map[lk]
                val = rec[k]
                if val not in (None, ""):
                    return val
        return None

    id_ = get_any("_id", "id", "Id", "ID", "serial_no", "serialNo", "HotelID", "hotel_id")

    if lang == "zh":
        name = get_any("旅宿名稱",
                       "name", "Name", "hotel_name", "HotelName", "Hotel_Name",
                       "旅館名稱", "中文名稱", "名稱",
                       "chs_name", "name_zh", "nameZh", "nameCn", "nameCN", "name_ch",
                       "ChineseName")
    else:  
        name = get_any("hotel name",            
                       "EnglishName", "name", "Name", "HotelName", "hotel_name")

    if lang == "zh":
        address = get_any("地址",
                          "address", "Address", "addr", "Addr", "HotelAddress",
                          "中文地址", "chs_address", "address_zh", "address_ch")
    else:
        address = get_any("address",            
                          "EnglishAddress", "HotelAddress", "addr", "Addr")
    if lang == "zh":
        phone = get_any("電話或手機號碼",
                        "phone", "Phone", "TEL", "Tel", "telephone", "Telephone", "連絡電話", "聯絡電話")
    else:
        phone = get_any("tel",                
                        "telephone", "Telephone", "phone", "Phone", "TEL", "Tel")

    if lang == "zh":
        rooms_raw = get_any("房間數",
                            "rooms", "Rooms", "roomCount", "room_count",
                            "總房間數", "客房數", "total_rooms", "TotalRooms", "TotalNumberOfRooms",
                            "rooms_total", "room_total", "RoomNumber", "RoomCount", "RoomTotal")
    else:
        rooms_raw = get_any("the total number of rooms",   
                            "total number of rooms", "TotalRooms", "total_rooms",
                            "rooms", "Rooms", "roomCount", "room_count")

    district = get_any("district", "District", "行政區", "鄉鎮市區", "zone", "area")
    if not district and lang == "zh":
        if isinstance(address, str):
            m = re.search(r"(中正區|大同區|中山區|松山區|大安區|萬華區|信義區|士林區|北投區|內湖區|南港區|文山區)", address)
            if m:
                district = m.group(1)

    def to_int(x):
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return int(x)
        s = re.sub(r"[^\d]", "", str(x))
        return int(s) if s.isdigit() else None

    return {
        "id": (str(id_).strip() if id_ is not None else None),
        f"name_{lang}": (str(name).strip() if name is not None else None),
        f"address_{lang}": (str(address).strip() if address is not None else None),
        "district": (str(district).strip() if district is not None else None),
        "rooms": to_int(rooms_raw),
        "phone": (str(phone).strip() if phone is not None else None),
        "_raw_name": (str(name).strip() if name is not None else None),
    }


def normalize_name_key(name: str) -> str:
    s = (name or "")
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", "", s).lower()
    s = re.sub(r"[．\.\-–—_•·,，、!！()?？（）\[\]【】]", "", s)
    return s

def merge_cn_en(cn_list, en_list):
    def build_index_by_id_or_name(rows):
        by_id, by_name = {}, {}
        for r in rows:
            if r.get("id"):
                by_id[r["id"]] = r
            if r.get("_raw_name"):
                by_name[normalize_name_key(r["_raw_name"])] = r
        return by_id, by_name

    cn_by_id, cn_by_name = build_index_by_id_or_name(cn_list)
    en_by_id, en_by_name = build_index_by_id_or_name(en_list)

    merged = []
    for cn in cn_list:
        out = {
            "id": cn.get("id"),
            "name_ch": cn.get("name_zh"),
            "name_en": None,
            "address_ch": cn.get("address_zh"),
            "address_en": None,
            "district": cn.get("district"),
            "rooms": cn.get("rooms"),
            "phone": cn.get("phone"),
        }

        en = None
        if cn.get("id") and cn["id"] in en_by_id:
            en = en_by_id[cn["id"]]
        elif cn.get("_raw_name"):
            en = en_by_name.get(normalize_name_key(cn["_raw_name"]))

        if en:
            out["name_en"] = out["name_en"] or en.get("name_en")
            out["address_en"] = out["address_en"] or en.get("address_en")
            if not out["district"] and en.get("district"):
                out["district"] = en.get("district")
            if out["rooms"] is None and en.get("rooms") is not None:
                out["rooms"] = en.get("rooms")
            if not out["phone"] and en.get("phone"):
                out["phone"] = en.get("phone")

        merged.append(out)
    return merged

def main():
    print(">>> 程式開始執行", flush=True)
    print(">>> 正在執行檔案：", os.path.abspath(__file__), flush=True)

    try:
        print(">>> 下載 ZH/EN 原始文字...", flush=True)
        zh_raw = fetch_text(URL_ZH)
        en_raw = fetch_text(URL_EN)
        print(">>> 下載完成", flush=True)
    except Exception as e:
        print("下載失敗：", e, flush=True)
        traceback.print_exc()
        return

    try:
        print(">>> 解析原始文字為 list...", flush=True)
        zh_list_raw = parse_payload_to_list(zh_raw)
        en_list_raw = parse_payload_to_list(en_raw)
        print(">>> 解析完成", flush=True)
    except Exception as e:
        print("解析失敗：", e, flush=True)
        traceback.print_exc()
        return

    print(">>> Peek 原始結構", flush=True)
    peek_sample("ZH", zh_list_raw)
    peek_sample("EN", en_list_raw)

    print(">>> 正規化欄位...", flush=True)
    zh_norm = [norm_record(rec, "zh") for rec in zh_list_raw]
    en_norm = [norm_record(rec, "en") for rec in en_list_raw]

    print(">>> 合併中/英資料...", flush=True)
    merged = merge_cn_en(zh_norm, en_norm)
    print(">>> 合併完成，筆數：", len(merged), flush=True)

    print(f">>> 輸出 {OUT_HOTELS} ...", flush=True)
    with open(OUT_HOTELS, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["ChineseName", "EnglishName", "ChineseAddress", "EnglishAddress", "Phone", "RoomCount"])
        for r in merged:
            writer.writerow([
                r.get("name_ch") or "",
                r.get("name_en") or "",
                r.get("address_ch") or "",
                r.get("address_en") or "",
                r.get("phone") or "",
                r.get("rooms") if isinstance(r.get("rooms"), int) else ""
            ])
    print(">>> hotels.csv 完成", flush=True)

    print(">>> 聚合 districts...", flush=True)
    agg = {}
    for r in merged:
        dist = r.get("district") or "未知/未填"
        rooms = r.get("rooms") if isinstance(r.get("rooms"), int) else 0
        if dist not in agg:
            agg[dist] = {"hotel_count": 0, "total_rooms": 0}
        agg[dist]["hotel_count"] += 1
        agg[dist]["total_rooms"] += rooms

    print(f">>> 輸出 {OUT_DISTRICTS} ...", flush=True)
    with open(OUT_DISTRICTS, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["DistrictName", "HotelCount", "RoomCount"])
        for dist, v in sorted(agg.items(), key=lambda x: x[0]):
            writer.writerow([dist, v["hotel_count"], v["total_rooms"]])
    print(">>> districts.csv 完成", flush=True)

    missing_ch_name = sum(1 for r in merged if not r.get("name_ch"))
    missing_en_name = sum(1 for r in merged if not r.get("name_en"))
    missing_rooms   = sum(1 for r in merged if r.get("rooms") is None)
    print(f"[DEBUG] 缺中文名: {missing_ch_name} 筆, 缺英文名: {missing_en_name} 筆, 缺房間數: {missing_rooms} 筆", flush=True)

    print(">>> 全部完成", flush=True)

if __name__ == "__main__":
    main()
