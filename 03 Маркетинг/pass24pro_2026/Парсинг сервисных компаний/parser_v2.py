"""
PASS24 Market — Парсер сервисных компаний v2
Поиск: DuckDuckGo (бесплатно, без API-ключа)
Скрапинг: requests + BeautifulSoup + regex
"""

import os
import sys
import json
import time
import re
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

import requests
from bs4 import BeautifulSoup
import pandas as pd
from duckduckgo_search import DDGS

if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf-8-sig"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─── Конфигурация ───────────────────────────────────────────────────────────

load_dotenv()

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TODAY = date.today().strftime("%Y-%m-%d")

# Сколько URL брать из поиска на один запрос
SEARCH_LIMIT = 8

# Пауза между запросами к сайтам (чтобы не получить бан)
PAUSE_SCRAPE = 2

# Таймаут загрузки страницы
HTTP_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

# Домены-агрегаторы — пропускаем
SKIP_DOMAINS = {
    "avito.ru", "yandex.ru", "google.com", "zoon.ru", "profi.ru",
    "youdo.com", "2gis.ru", "vk.com", "instagram.com", "ok.ru",
    "hh.ru", "headhunter.ru", "otzovik.com", "irecommend.ru",
    "flamp.ru", "tripadvisor.ru", "wikipedia.org", "youtube.com",
    "dzen.ru", "mail.ru", "rambler.ru", "bing.com", "duckduckgo.com",
}

# ─── Поисковые запросы ────────────────────────────────────────────────────────

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

# ─── Поиск через DuckDuckGo ──────────────────────────────────────────────────

def is_skip_domain(url: str) -> bool:
    return any(d in url for d in SKIP_DOMAINS)


def search_urls(query: str, limit: int = SEARCH_LIMIT) -> list[dict]:
    """Поиск через DuckDuckGo, возвращает список {url, title, description}."""
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, region="ru-ru", max_results=limit):
                url = r.get("href", "")
                if url and not is_skip_domain(url):
                    results.append({
                        "url": url,
                        "title": r.get("title", ""),
                        "description": r.get("body", ""),
                    })
    except Exception as e:
        print(f"     ✗ DDG error: {e}")
    return results


# ─── Regex-паттерны ──────────────────────────────────────────────────────────

PHONE_RE = re.compile(
    r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}"
)
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")
VK_RE    = re.compile(r"https?://(?:www\.)?vk\.com/[\w.\-]+")
TG_RE    = re.compile(r"https?://(?:www\.)?t(?:elegram)?\.me/[\w.\-]+|@[\w]{5,}")

SUBURBS_KW = [
    "подмосковье", "московская область", "мо ", " мо,", "загород",
    "коттедж", "дача", "посёлок", "поселок", "ленинградская",
]


# ─── Скрапинг и извлечение контактов ─────────────────────────────────────────

def scrape_company(url: str, category: str) -> dict | None:
    """
    Загружает страницу и извлекает контакты через regex + BeautifulSoup.
    Использует meta-теги, schema.org, href=tel:/mailto: и текст страницы.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        print(f"     ✗ Fetch error: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Название компании ─────────────────────────────────────────────────
    company_name = ""
    # 1. schema.org
    for tag in soup.find_all(attrs={"itemprop": "name"}):
        t = tag.get_text(strip=True)
        if t:
            company_name = t
            break
    # 2. og:site_name
    if not company_name:
        og = soup.find("meta", property="og:site_name")
        if og:
            company_name = og.get("content", "").strip()
    # 3. <title>
    if not company_name and soup.title:
        company_name = soup.title.string or ""
        company_name = company_name.split("|")[0].split("—")[0].split("-")[0].strip()

    if not company_name:
        return None

    # ── Полный текст страницы для поиска ──────────────────────────────────
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    full_text = soup.get_text(separator=" ")

    # ── Телефон ───────────────────────────────────────────────────────────
    phone = ""
    # Сначала ищем в href="tel:..."
    tel_links = soup.find_all("a", href=re.compile(r"^tel:"))
    if tel_links:
        phone = tel_links[0]["href"].replace("tel:", "").strip()
    # Иначе — regex по тексту
    if not phone:
        m = PHONE_RE.search(full_text)
        if m:
            phone = m.group(0)

    # ── Email ─────────────────────────────────────────────────────────────
    email = ""
    mail_links = soup.find_all("a", href=re.compile(r"^mailto:"))
    if mail_links:
        email = mail_links[0]["href"].replace("mailto:", "").split("?")[0].strip()
    if not email:
        m = EMAIL_RE.search(full_text)
        if m:
            email = m.group(0)

    # ── VK / Telegram ─────────────────────────────────────────────────────
    all_links = [a.get("href", "") for a in soup.find_all("a", href=True)]
    vk_url = next((l for l in all_links if "vk.com/" in l), "")
    telegram = next(
        (l for l in all_links if "t.me/" in l or "telegram.me/" in l), ""
    )
    if not telegram:
        m = TG_RE.search(full_text)
        if m:
            telegram = m.group(0)

    # ── Адрес ─────────────────────────────────────────────────────────────
    address = ""
    for tag in soup.find_all(attrs={"itemprop": "address"}):
        t = tag.get_text(strip=True)
        if t:
            address = t[:150]
            break

    # ── Описание ─────────────────────────────────────────────────────────
    description = ""
    og_desc = soup.find("meta", property="og:description") or soup.find("meta", attrs={"name": "description"})
    if og_desc:
        description = og_desc.get("content", "").strip()[:200]

    # ── Работает в Подмосковье? ───────────────────────────────────────────
    check_text = (company_name + " " + description + " " + full_text[:2000]).lower()
    works_in_suburbs = any(k in check_text for k in SUBURBS_KW)

    return {
        "company_name":     company_name[:100],
        "phone":            phone,
        "email":            email,
        "website":          url,
        "vk_url":           vk_url,
        "telegram":         telegram,
        "rating":           None,
        "reviews_count":    None,
        "address":          address,
        "description":      description,
        "works_in_suburbs": works_in_suburbs,
        "category":         category,
        "source_url":       url,
        "source":           "scrape",
    }


# ─── Fallback: извлечение из метаданных поиска ───────────────────────────────

def company_from_meta(item: dict, category: str) -> dict | None:
    """Карточка компании из метаданных поиска (без скрапинга)."""
    url   = item.get("url", "")
    title = item.get("title", "").strip()
    desc  = item.get("description", "").strip()
    if not title:
        return None
    phone_m = PHONE_RE.search(desc)
    return {
        "company_name":     title,
        "phone":            phone_m.group(0) if phone_m else "",
        "email":            "",
        "website":          url,
        "vk_url":           "",
        "telegram":         "",
        "rating":           None,
        "reviews_count":    None,
        "address":          "",
        "description":      desc[:200],
        "works_in_suburbs": any(k in (title + desc).lower() for k in SUBURBS_KW),
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
    if row.get("works_in_suburbs"):                                  score += 3
    if (row.get("rating") or 0) >= 4.5:                             score += 2
    if (row.get("reviews_count") or 0) >= 15:                       score += 2
    if row.get("website") and "http" in str(row.get("website", "")): score += 1
    if row.get("email"):                                             score += 1
    if row.get("vk_url") or row.get("telegram"):                     score += 1
    if row.get("phone"):                                             score += 1
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
        df = pd.concat(
            [df[has_phone].drop_duplicates(subset=["phone"], keep="first"),
             df[~has_phone]],
            ignore_index=True,
        )
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
    df = df.sort_values(["_sort", "company_name"], ascending=[True, True])
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

    sep = "=" * 60
    print(f"\n{sep}")
    print("  РЕЗУЛЬТАТЫ")
    print(sep)
    print(f"  Всего компаний:  {len(df)}")
    for p in ["P1", "P2", "P3"]:
        print(f"  Приоритет {p}:    {len(df[df['priority'] == p])}")
    print()
    print("  По категориям:")
    for cat, count in df["category"].value_counts().items():
        p1 = len(df[(df["category"] == cat) & (df["priority"] == "P1")])
        print(f"    {cat:<25} {count:>4}  (P1: {p1})")
    print()
    print(f"  [Excel]  {xlsx_path}")
    print(f"  [CSV]    {csv_path}")
    print(sep)


# ─── Главная функция ─────────────────────────────────────────────────────────

def main():
    sep = "=" * 60
    print(sep)
    print("  PASS24 Market — Парсер v2 (DDG + Claude)")
    print(f"  Дата: {TODAY}")
    print(sep)

    all_companies: list[dict] = []
    seen_urls: set[str] = set()
    candidate_urls: list[tuple[str, str, dict]] = []

    # ── Этап 1: Поиск URL через DuckDuckGo ───────────────────────────────
    print("\n[1/2] Поиск сайтов через DuckDuckGo...")

    for category, queries in SEARCH_QUERIES.items():
        print(f"\n  Категория: {category}")
        cat_count = 0
        for query in queries:
            print(f"  → {query!r}")
            results = search_urls(query)
            for r in results:
                url = r["url"]
                if url not in seen_urls:
                    seen_urls.add(url)
                    candidate_urls.append((url, category, r))
                    cat_count += 1
            time.sleep(1)  # небольшая пауза чтобы не перегружать DDG
        print(f"     ✓ Найдено URL: {cat_count}")

    print(f"\n  Всего уникальных URL: {len(candidate_urls)}")

    # ── Этап 2: Скрапинг + Claude ─────────────────────────────────────────
    print(f"\n[2/2] Скрапинг и извлечение контактов...")
    total = len(candidate_urls)

    for i, (url, category, meta) in enumerate(candidate_urls, 1):
        print(f"  [{i}/{total}] {url[:70]}")

        company = scrape_company(url, category)

        if company:
            all_companies.append(company)
            phone = company.get("phone") or "—"
            print(f"     ✓ {company.get('company_name', '?')} | {phone}")
        else:
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
