"""
PASS24 Market — Обработчик JSON-экспортов из Firecrawl Playground

Принимает JSON-файл из Firecrawl, обогащает данные (парсит сайты за контактами)
и сохраняет результат в папку data/.

Запуск:
  py process_json.py                          # спросит путь к файлу
  py process_json.py path/to/file.json        # передать файл напрямую
  py process_json.py path/to/file.json --enrich  # + парсинг сайтов за контактами
"""

import sys
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf-8-sig"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import json
import time
import re
import os
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

import requests
import pandas as pd

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
BASE_URL = "https://api.firecrawl.dev/v1"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TODAY = date.today().strftime("%Y-%m-%d")

# ─── Классификация по ключевым словам ───────────────────────────────────────

CATEGORY_KEYWORDS = {
    "Клининг":          ["клинин", "уборк", "мытье", "мойк", "чистк", "clean"],
    "Уход за садом":    ["сад", "ландшафт", "газон", "озеленен", "деревь", "кустарник", "садовник"],
    "Сантехника":       ["сантехник", "водопровод", "канализац", "труб", "котел", "насос"],
    "Электрика":        ["электрик", "электромонтаж", "проводк", "электроснабжен"],
    "Строительство":    ["баня", "отоплен", "строительств", "ремонт", "фундамент", "кровля", "фасад"],
    "Бассейны":         ["бассейн", "spa", "спа", "гидромассаж"],
    "Комплексный сервис": ["комплексн", "обслуживан", "управлен", "техническ"],
    "Вывоз мусора":     ["мусор", "вывоз", "уборка территор", "КГМ"],
    "Цветы":            ["цветы", "цветов", "флорист", "букет", "подписк"],
    "Няни":             ["няня", "нян", "ребенок", "детск"],
    "Доставка":         ["доставка", "привоз", "дрова", "вода"],
}

# Домены, которые не являются целевыми компаниями
SKIP_DOMAINS = [
    "avito.ru", "businessmens.ru", "mapwork.ru", "hh.ru",
    "headhunter.ru", "superjob.ru", "youdo.com",
    "2gis.ru", "yandex.ru", "google.com", "wikipedia.org",
]

ENRICH_PROMPT = """
Extract contact information from this company website.
Return JSON with these fields:
- company_name: official company name
- phone: main phone number (Russian format)
- email: contact email
- vk_url: VKontakte URL if present
- telegram: Telegram username or link if present
- whatsapp: WhatsApp number if present
- address: physical address or service area
- rating: numeric rating if shown
- reviews_count: number of reviews if shown
- works_in_suburbs: true if company mentions загород / МО / подмосковье / 
  коттедж / дача / коттеджный посёлок / Московская область
- description: brief description of what the company does (1-2 sentences)

Return only JSON object. If field not found, use null.
"""

ENRICH_SCHEMA = {
    "type": "object",
    "properties": {
        "company_name":     {"type": "string"},
        "phone":            {"type": "string"},
        "email":            {"type": "string"},
        "vk_url":           {"type": "string"},
        "telegram":         {"type": "string"},
        "whatsapp":         {"type": "string"},
        "address":          {"type": "string"},
        "rating":           {"type": "number"},
        "reviews_count":    {"type": "integer"},
        "works_in_suburbs": {"type": "boolean"},
        "description":      {"type": "string"},
    }
}

# ─── Вспомогательные функции ─────────────────────────────────────────────────

def detect_category(title: str, description: str) -> str:
    text = (title + " " + (description or "")).lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "Прочее"


def is_relevant(url: str, title: str, description: str) -> bool:
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lstrip("www.")
    if any(skip in domain for skip in SKIP_DOMAINS):
        return False
    text = (title + " " + (description or "")).lower()
    irrelevant = ["вакансии", "бизнес-идеи", "бизнес идеи", "объявлени", "работа на дому"]
    if any(w in text for w in irrelevant):
        return False
    return True


def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits[0] in ("7", "8"):
        return f"+7{digits[1:]}"
    if len(digits) == 10:
        return f"+7{digits}"
    return phone


def assign_priority(row: dict) -> str:
    score = 0
    if row.get("works_in_suburbs"):                      score += 3
    if (row.get("rating") or 0) >= 4.5:                 score += 2
    if (row.get("reviews_count") or 0) >= 15:           score += 2
    if row.get("website") and "http" in str(row.get("website", "")):
                                                         score += 1
    if row.get("email"):                                 score += 1
    if row.get("vk_url") or row.get("telegram"):        score += 1
    if score >= 7:   return "P1"
    elif score >= 4: return "P2"
    else:            return "P3"


# ─── Обогащение данных через Firecrawl ──────────────────────────────────────

def enrich_url(url: str) -> dict:
    """Скрапит URL и извлекает контактные данные."""
    print(f"    enriching: {url}")
    try:
        resp = requests.post(
            f"{BASE_URL}/scrape",
            headers={
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "url": url,
                "formats": ["extract"],
                "extract": {
                    "prompt": ENRICH_PROMPT,
                    "schema": ENRICH_SCHEMA,
                }
            },
            timeout=45,
        )
        resp.raise_for_status()
        data = resp.json()
        extract = data.get("data", {}).get("extract", {})
        return extract or {}
    except Exception as e:
        print(f"      error: {e}")
        return {}


# ─── Основная обработка ──────────────────────────────────────────────────────

def process_firecrawl_json(json_path: Path, do_enrich: bool = False) -> pd.DataFrame:
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    # Поддерживаем формат Playground: data["web"] или data["data"]
    raw_results = data.get("web") or data.get("data") or []
    query = data.get("formState", {}).get("url", "")

    print(f"\nЗапрос:  {query[:100]}")
    print(f"Найдено: {len(raw_results)} результатов")

    rows = []
    for item in raw_results:
        url   = item.get("url", "")
        title = item.get("title", "")
        desc  = item.get("description", "") or item.get("markdown", "")[:300]

        if not is_relevant(url, title, desc):
            print(f"  [skip] {url}")
            continue

        category = detect_category(title, desc)

        row = {
            "category":     category,
            "company_name": title,
            "website":      url,
            "description":  desc,
            "source":       "Firecrawl Search",
            "source_url":   url,
            # Поля для обогащения (заполнятся ниже или останутся пустыми)
            "phone":            "",
            "email":            "",
            "vk_url":           "",
            "telegram":         "",
            "whatsapp":         "",
            "address":          "",
            "rating":           None,
            "reviews_count":    None,
            "works_in_suburbs": None,
        }

        if do_enrich and FIRECRAWL_API_KEY:
            enriched = enrich_url(url)
            for field in ["company_name", "phone", "email", "vk_url", "telegram",
                          "whatsapp", "address", "rating", "reviews_count",
                          "works_in_suburbs", "description"]:
                val = enriched.get(field)
                if val not in (None, "", []):
                    row[field] = val
            row["phone"] = normalize_phone(row["phone"])
            time.sleep(1)

        rows.append(row)
        print(f"  [ok]   {url}  ->  {category}")

    if not rows:
        print("\n[!] Нет релевантных результатов после фильтрации.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["priority"] = df.apply(lambda r: assign_priority(r.to_dict()), axis=1)
    df["status"] = "Не обработан"
    df["contact_date"] = ""
    df["manager_comment"] = ""

    # Порядок столбцов
    cols = [
        "priority", "category", "company_name", "phone", "email",
        "website", "vk_url", "telegram", "whatsapp",
        "rating", "reviews_count", "address", "works_in_suburbs",
        "description", "source",
        "status", "contact_date", "manager_comment",
    ]
    existing = [c for c in cols if c in df.columns]
    df = df[existing]

    priority_order = {"P1": 0, "P2": 1, "P3": 2}
    df["_sort"] = df["priority"].map(priority_order)
    df = df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    df.index += 1

    return df


def save_df(df: pd.DataFrame, source_name: str):
    safe_name = re.sub(r'[\\/:*?"<>|]', "_", source_name)[:40]
    xlsx_path = DATA_DIR / f"{safe_name}_{TODAY}.xlsx"
    csv_path  = DATA_DIR / f"{safe_name}_{TODAY}.csv"

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Все компании", index=True)
        for priority in ["P1", "P2", "P3"]:
            sub = df[df["priority"] == priority]
            if not sub.empty:
                sub.to_excel(writer, sheet_name=priority, index=True)
        for cat in df["category"].unique():
            sub = df[df["category"] == cat]
            if not sub.empty:
                sub.to_excel(writer, sheet_name=cat[:31], index=True)

    df.to_csv(csv_path, index=True, encoding="utf-8-sig")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  РЕЗУЛЬТАТ ОБРАБОТКИ")
    print(sep)
    print(f"  Компаний после фильтрации: {len(df)}")
    for p in ["P1", "P2", "P3"]:
        cnt = len(df[df["priority"] == p])
        print(f"  {p}: {cnt}")
    print()
    print("  По категориям:")
    for cat, cnt in df["category"].value_counts().items():
        print(f"    {cat:<28} {cnt}")
    print()
    print(f"  [Excel]  {xlsx_path}")
    print(f"  [CSV]    {csv_path}")
    print(sep)


# ─── Точка входа ─────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    do_enrich = "--enrich" in args
    args = [a for a in args if a != "--enrich"]

    if args:
        json_path = Path(args[0])
    else:
        default = Path(r"C:\Users\ekorn\Downloads")
        # Найти последний JSON-файл в папке Downloads
        candidates = sorted(default.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if candidates:
            print(f"Найден файл: {candidates[0].name}")
            answer = input("Использовать его? [Enter = да / введите путь]: ").strip()
            json_path = Path(answer) if answer else candidates[0]
        else:
            json_path = Path(input("Введите путь к JSON-файлу: ").strip().strip('"'))

    if not json_path.exists():
        print(f"[ERROR] Файл не найден: {json_path}")
        return

    if do_enrich and not FIRECRAWL_API_KEY:
        print("[!] --enrich запрошен, но FIRECRAWL_API_KEY не задан.")
        print("    Продолжаем без обогащения.")
        do_enrich = False

    sep = "=" * 60
    print(sep)
    print("  PASS24 Market -- Обработчик JSON Firecrawl")
    print(f"  Файл: {json_path.name}")
    print(f"  Режим: {'с обогащением (enrich)' if do_enrich else 'только структурирование'}")
    print(sep)

    df = process_firecrawl_json(json_path, do_enrich=do_enrich)

    if not df.empty:
        save_df(df, json_path.stem[:40])
    else:
        print("Нет данных для сохранения.")


if __name__ == "__main__":
    main()
