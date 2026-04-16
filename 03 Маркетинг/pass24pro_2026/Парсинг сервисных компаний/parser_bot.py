"""
PASS24 Market — Парсер сервисных компаний Москвы и МО
Использует Firecrawl API для сбора базы партнёров для pass24market.ru

Стратегия:
  1. /search → находит URL сайтов реальных компаний по поисковым запросам
  2. /scrape (markdown) → снимает контакты с каждого найденного сайта
  3. Дедупликация, приоритизация, сохранение в Excel/CSV

Запуск: py parser_bot.py
"""

import os
import sys
import json
import time
import re
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf-8-sig"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests
import pandas as pd

# ─── Конфигурация ───────────────────────────────────────────────────────────

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
BASE_URL = "https://api.firecrawl.dev/v1"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

TODAY = date.today().strftime("%Y-%m-%d")

# Паузы между запросами (free plan: 10 req/min)
PAUSE_SEARCH  = 7   # между поисковыми запросами
PAUSE_SCRAPE  = 8   # между скрапингом сайтов

# Сколько URL брать из поиска на категорию
SEARCH_LIMIT = 8

# Домены-агрегаторы — пропускаем, нам нужны сайты самих компаний
SKIP_DOMAINS = {
    "avito.ru", "yandex.ru", "google.com", "zoon.ru", "profi.ru",
    "youdo.com", "2gis.ru", "vk.com", "instagram.com", "ok.ru",
    "hh.ru", "headhunter.ru", "ivi.ru", "otzovik.com", "irecommend.ru",
    "flamp.ru", "tripadvisor.ru", "wikipedia.org", "youtube.com",
    "dzen.ru", "mail.ru", "ok.ru", "rambler.ru",
}

# ─── Поисковые запросы по категориям ────────────────────────────────────────

SEARCH_QUERIES: dict[str, list[str]] = {
    "Клининг": [
        "уборка коттеджей Подмосковье компания сайт телефон",
        "клининг загородного дома МО профессиональная уборка",
        "уборка дачного дома после зимы московская область",
        "клининговая компания выезд за город подмосковье",
    ],
    "Уход за садом": [
        "уход за садом коттеджный посёлок МО компания",
        "стрижка газона обслуживание участка подмосковье",
        "ландшафтный уход коттедж Московская область выезд",
    ],
    "Сантехника": [
        "сантехник выезд подмосковье загородный дом коттедж",
        "сантехнические услуги коттеджный посёлок МО круглосуточно",
        "ремонт водоснабжения отопления загородный дом Москва область",
    ],
    "Электрика": [
        "электрик загородный дом Московская область коттедж выезд",
        "электромонтаж частный дом МО аварийный вызов",
    ],
    "Вывоз мусора": [
        "вывоз мусора КГМ коттеджный посёлок МО компания",
        "вывоз строительного мусора подмосковье загород",
    ],
    "Бассейны": [
        "обслуживание бассейна коттедж подмосковье компания",
        "чистка бассейна выезд московская область",
    ],
    "Няни": [
        "агентство нянь загородный дом МО коттеджный посёлок",
        "няня с проживанием коттеджный посёлок подмосковье",
    ],
    "Химчистка": [
        "химчистка с выездом загородный дом подмосковье коттедж",
        "выездная химчистка ковров мягкой мебели МО",
    ],
    "Ветеринар": [
        "ветеринар на дом коттеджный посёлок МО выезд",
        "ветеринарная клиника выезд загородный дом подмосковье",
    ],
    "Дрова": [
        "доставка дров коттеджный посёлок московская область",
        "дрова с доставкой на дом МО частный дом",
    ],
}

# ─── Промпт и схема для /scrape LLM extract ─────────────────────────────────

COMPANY_EXTRACT_PROMPT = """
This is the website of a service company. Extract contact information.
Return JSON with these fields:
- company_name: company name (string)
- phone: main phone number (string, Russian format)
- email: email address (string)
- address: address or service area (string)
- description: what services they provide, 1-2 sentences (string)
- works_in_suburbs: true if they serve загород / МО / подмосковье / коттедж (boolean)
- rating: numeric rating if shown on page (number or null)
- vk_url: VKontakte page URL (string)
- telegram: Telegram username or link (string)

If a field is not found, return null or empty string.
Return only the JSON object, no explanation.
"""

# ─── Firecrawl API ──────────────────────────────────────────────────────────

def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json",
    }


def search_urls(query: str, limit: int = SEARCH_LIMIT) -> list[dict]:
    """Поиск через Firecrawl /search — возвращает список {url, title, description}."""
    try:
        resp = requests.post(
            f"{BASE_URL}/search",
            headers=get_headers(),
            json={"query": query, "limit": limit},
            timeout=45,
        )
        resp.raise_for_status()
        results = resp.json().get("data", [])
        return [
            {"url": r.get("url", ""), "title": r.get("metadata", {}).get("title", ""),
             "description": r.get("metadata", {}).get("description", "")}
            for r in results if r.get("url")
        ]
    except requests.HTTPError as e:
        code = e.response.status_code
        if code == 429:
            print(f"     ⏳ Rate limit — жду 40 секунд...")
            time.sleep(40)
        else:
            print(f"     ✗ HTTP {code}")
        return []
    except Exception as e:
        print(f"     ✗ Search error: {e}")
        return []


def scrape_company_site(url: str) -> dict | None:
    """Скрапит страницу компании и извлекает контакты через LLM extract."""
    try:
        resp = requests.post(
            f"{BASE_URL}/scrape",
            headers=get_headers(),
            json={
                "url": url,
                "formats": ["extract"],
                "extract": {"prompt": COMPANY_EXTRACT_PROMPT},
                "onlyMainContent": False,
                "timeout": 20000,
            },
            timeout=40,
        )
        resp.raise_for_status()
        data = resp.json()
        extract = (data.get("data") or {}).get("extract") or {}

        # extract может прийти как строка-JSON
        if isinstance(extract, str):
            try:
                extract = json.loads(extract)
            except Exception:
                return None

        if not extract or not extract.get("company_name"):
            return None

        extract["source_url"] = url
        return extract

    except requests.HTTPError as e:
        code = e.response.status_code
        if code == 429:
            print(f"     ⏳ Rate limit — жду 40 секунд...")
            time.sleep(40)
        return None
    except Exception:
        return None


def is_skip_domain(url: str) -> bool:
    for d in SKIP_DOMAINS:
        if d in url:
            return True
    return False


# ─── Извлечение компании из метаданных поиска (без скрапинга) ───────────────

PHONE_RE = re.compile(
    r"(\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}"
)

def company_from_meta(item: dict, category: str) -> dict | None:
    """Формирует карточку компании из метаданных поиска (title + description)."""
    url   = item.get("url", "")
    title = item.get("title", "").strip()
    desc  = item.get("description", "").strip()

    if not title:
        return None

    phone_match = PHONE_RE.search(desc)
    phone = phone_match.group(0) if phone_match else ""

    suburbs_keywords = [
        "подмосковье", "московская область", "мо ", " мо,", "загород",
        "коттедж", "дача", "посёлок", "поселок",
    ]
    works_in_suburbs = any(k in (title + desc).lower() for k in suburbs_keywords)

    return {
        "company_name":     title,
        "phone":            phone,
        "email":            "",
        "website":          url,
        "vk_url":           "",
        "telegram":         "",
        "rating":           None,
        "reviews_count":    None,
        "address":          "",
        "description":      desc[:200] if desc else "",
        "works_in_suburbs": works_in_suburbs,
        "category":         category,
        "source_url":       url,
        "source":           "search_meta",
    }


# ─── Нормализация и приоритеты ───────────────────────────────────────────────

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
    if row.get("works_in_suburbs"):                             score += 3
    if (row.get("rating") or 0) >= 4.5:                        score += 2
    if (row.get("reviews_count") or 0) >= 15:                  score += 2
    if row.get("website") and "http" in str(row.get("website", "")): score += 1
    if row.get("email"):                                        score += 1
    if row.get("vk_url") or row.get("telegram"):                score += 1
    if row.get("phone"):                                        score += 1

    if score >= 7:   return "P1"
    elif score >= 4: return "P2"
    else:            return "P3"


def process_companies(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    if "phone" in df.columns:
        df["phone"] = df["phone"].fillna("").apply(normalize_phone)

    df = df[df["company_name"].notna() & (df["company_name"] != "")]
    df = df.drop_duplicates(subset=["company_name"], keep="first")

    if "phone" in df.columns:
        has_phone = df["phone"] != ""
        df_with = df[has_phone].drop_duplicates(subset=["phone"], keep="first")
        df_without = df[~has_phone]
        df = pd.concat([df_with, df_without], ignore_index=True)

    df["priority"] = df.apply(lambda r: assign_priority(r.to_dict()), axis=1)
    df["status"] = "Не обработан"
    df["contact_date"] = ""
    df["manager_comment"] = ""

    cols_order = [
        "priority", "category", "company_name", "phone", "email",
        "website", "vk_url", "telegram", "rating", "reviews_count",
        "address", "works_in_suburbs", "description",
        "source", "source_url", "status", "contact_date", "manager_comment",
    ]
    existing = [c for c in cols_order if c in df.columns]
    df = df[existing]

    priority_map = {"P1": 0, "P2": 1, "P3": 2}
    df["_sort"] = df["priority"].map(priority_map)
    df = df.sort_values(["_sort", "reviews_count"], ascending=[True, False])
    df = df.drop(columns=["_sort"]).reset_index(drop=True)
    df.index += 1
    return df


# ─── Сохранение ─────────────────────────────────────────────────────────────

def save_results(df: pd.DataFrame, all_raw: list[dict]):
    if df.empty:
        print("\n⚠ Нет данных для сохранения.")
        return

    xlsx_path = DATA_DIR / f"companies_{TODAY}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Все компании", index=True)
        for p in ["P1", "P2", "P3"]:
            sub = df[df["priority"] == p]
            if not sub.empty:
                sub.to_excel(writer, sheet_name=p, index=True)
        for cat in df["category"].unique():
            sub = df[(df["category"] == cat) & (df["priority"].isin(["P1", "P2"]))]
            if not sub.empty:
                sub.to_excel(writer, sheet_name=cat[:31], index=True)

    csv_path = DATA_DIR / f"companies_{TODAY}.csv"
    df.to_csv(csv_path, index=True, encoding="utf-8-sig")

    json_path = DATA_DIR / f"companies_raw_{TODAY}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_raw, f, ensure_ascii=False, indent=2)

    print_summary(df, xlsx_path, csv_path)


def print_summary(df: pd.DataFrame, xlsx_path: Path, csv_path: Path):
    sep = "=" * 60
    print(f"\n{sep}")
    print("  РЕЗУЛЬТАТЫ ПАРСИНГА")
    print(sep)
    print(f"  Всего компаний:    {len(df)}")
    print(f"  Приоритет P1:      {len(df[df['priority']=='P1'])}")
    print(f"  Приоритет P2:      {len(df[df['priority']=='P2'])}")
    print(f"  Приоритет P3:      {len(df[df['priority']=='P3'])}")
    print()
    print("  По категориям:")
    for cat, count in df["category"].value_counts().items():
        p1 = len(df[(df["category"] == cat) & (df["priority"] == "P1")])
        print(f"    {cat:<25} {count:>4}  (P1: {p1})")
    print()
    print(f"  [Excel]  {xlsx_path}")
    print(f"  [CSV]    {csv_path}")
    print(sep)


# ─── Главная функция ────────────────────────────────────────────────────────

def main():
    sep = "=" * 60
    print(sep)
    print("  PASS24 Market -- Парсер сервисных компаний v2")
    print(f"  Дата: {TODAY}")
    print(sep)

    if not FIRECRAWL_API_KEY:
        print("\n[ОШИБКА] Не задан FIRECRAWL_API_KEY")
        print("   Создайте .env с: FIRECRAWL_API_KEY=fc-...")
        return

    all_companies: list[dict] = []
    seen_urls: set[str] = set()

    # ── Этап 1: Поиск URL компаний ────────────────────────────────────────
    print("\n[1/2] Поиск сайтов компаний...")
    candidate_urls: list[tuple[str, str, dict]] = []  # (url, category, meta)

    for category, queries in SEARCH_QUERIES.items():
        print(f"\n  Категория: {category}")
        cat_urls = 0
        for query in queries:
            print(f"  → Поиск: {query!r}")
            results = search_urls(query, limit=SEARCH_LIMIT)
            for r in results:
                url = r["url"]
                if url and not is_skip_domain(url) and url not in seen_urls:
                    seen_urls.add(url)
                    candidate_urls.append((url, category, r))
                    cat_urls += 1
            time.sleep(PAUSE_SEARCH)
        print(f"     ✓ Найдено URL: {cat_urls}")

    print(f"\n  Всего уникальных URL компаний: {len(candidate_urls)}")

    # ── Этап 2: Скрапинг каждого сайта ───────────────────────────────────
    print(f"\n[2/2] Извлечение контактов с сайтов...")
    total = len(candidate_urls)

    for i, (url, category, meta) in enumerate(candidate_urls, 1):
        print(f"  [{i}/{total}] {url[:70]}")
        company = scrape_company_site(url)

        if company:
            company["category"] = category
            company["source"] = "scrape"
            all_companies.append(company)
            print(f"     ✓ {company.get('company_name', '?')} | {company.get('phone', '—')}")
        else:
            # Fallback: берём данные из метаданных поиска
            fallback = company_from_meta(meta, category)
            if fallback:
                all_companies.append(fallback)
                print(f"     ~ (meta) {fallback['company_name'][:50]}")
            else:
                print(f"     ✗ Нет данных")

        time.sleep(PAUSE_SCRAPE)

    # ── Обработка и сохранение ────────────────────────────────────────────
    print(f"\n  Сырых записей: {len(all_companies)}")
    print("  Дедупликация и приоритизация...")
    df = process_companies(all_companies)
    save_results(df, all_companies)


if __name__ == "__main__":
    main()
