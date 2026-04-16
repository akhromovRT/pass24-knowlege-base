"""
PASS24 Market — Парсер сервисных компаний v3
=========================================
Источники (бесплатные, без API-ключей):
  1. DuckDuckGo — поисковые запросы по категориям
  2. Прямой скрапинг сайтов (requests + BeautifulSoup)

Новое по сравнению с v2:
  - Checkpoint/resume: прерванный парсинг продолжается с места остановки
  - Retry с экспоненциальным бэкофом на сетевые ошибки
  - Обнаружение контактной страницы (ищем /contacts, /о-нас, /about)
  - WhatsApp-ссылки в контактах
  - Дедупликация по домену (одна компания = один сайт)
  - Интерактивный выбор категорий при запуске

Запуск:
  py parser_v3.py                  # полный прогон по всем категориям
  py parser_v3.py --resume         # продолжить с checkpoint
  py parser_v3.py --category "Клининг"  # только одна категория
  py parser_v3.py --fast           # меньше URL, быстрее
"""

import os
import sys
import json
import time
import re
import argparse
import random
from datetime import date
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd
from duckduckgo_search import DDGS

if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf-8-sig"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─── Конфигурация ───────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TODAY = date.today().strftime("%Y-%m-%d")

CHECKPOINT_FILE = DATA_DIR / f"checkpoint_{TODAY}.json"

SEARCH_LIMIT    = 10   # URL на один поисковый запрос
PAUSE_DDG       = 2.5  # пауза между запросами к DDG (сек)
PAUSE_SCRAPE    = 2.0  # пауза между скрапингом сайтов (сек)
HTTP_TIMEOUT    = 18   # таймаут загрузки страницы (сек)
MAX_RETRY       = 3    # максимум попыток при ошибке сети

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Домены-агрегаторы — пропускаем
SKIP_DOMAINS = {
    "avito.ru", "yandex.ru", "yandex.com", "google.com", "google.ru",
    "zoon.ru", "profi.ru", "youdo.com", "2gis.ru", "vk.com",
    "instagram.com", "ok.ru", "hh.ru", "headhunter.ru",
    "otzovik.com", "irecommend.ru", "flamp.ru", "tripadvisor.ru",
    "wikipedia.org", "youtube.com", "dzen.ru", "mail.ru", "rambler.ru",
    "bing.com", "duckduckgo.com", "tiktok.com", "facebook.com",
    "rbc.ru", "kommersant.ru", "vc.ru", "habr.com", "pikabu.ru",
    "mapwork.ru", "businessmens.ru", "superjob.ru", "zarplata.ru",
    "rabota.ru", "work.ru", "domclick.ru", "cian.ru", "realty.yandex.ru",
}

# ─── Поисковые запросы ────────────────────────────────────────────────────────

SEARCH_QUERIES: dict[str, list[str]] = {
    "Клининг": [
        "клининговая компания выезд коттедж подмосковье сайт",
        "уборка загородного дома Московская область профессионально",
        "клининг дача коттеджный посёлок МО телефон заказать",
        "уборка после ремонта загородный дом Подмосковье компания",
        "генеральная уборка частный дом МО выезд",
    ],
    "Уход за садом": [
        "уход за садом коттеджный посёлок МО компания сайт",
        "стрижка газона обслуживание участка подмосковье выезд",
        "ландшафтный уход коттедж Московская область",
        "садовник выезд загородный дом МО регулярно",
    ],
    "Сантехника": [
        "сантехник выезд загородный дом коттедж Подмосковье",
        "сантехнические услуги коттеджный посёлок МО круглосуточно",
        "ремонт водоснабжения отопления частный дом МО компания",
        "аварийный сантехник частный дом московская область",
    ],
    "Электрика": [
        "электрик выезд загородный дом Московская область коттедж",
        "электромонтаж частный дом МО аварийный вызов компания",
        "замена проводки коттедж подмосковье мастер",
    ],
    "Вывоз мусора": [
        "вывоз мусора коттеджный посёлок Подмосковье компания",
        "вывоз строительного мусора загород МО организация",
        "мусорный контейнер аренда загородный участок МО",
    ],
    "Бассейны": [
        "обслуживание бассейна коттедж подмосковье компания",
        "чистка бассейна выезд московская область сезонное",
        "сервис бассейнов загородный дом МО",
    ],
    "Няни": [
        "агентство нянь загородный дом МО коттеджный посёлок",
        "няня с проживанием коттеджный посёлок подмосковье",
        "домработница с проживанием коттедж МО агентство",
    ],
    "Химчистка": [
        "химчистка выезд на дом загородный коттедж подмосковье",
        "выездная химчистка ковров мягкой мебели МО",
        "чистка диванов ковров выезд загород МО",
    ],
    "Ветеринар": [
        "ветеринар на дом коттеджный посёлок МО выезд",
        "ветеринарная клиника выезд загородный дом Подмосковье",
        "вызов ветеринара на дом Московская область",
    ],
    "Дрова": [
        "доставка дров коттеджный посёлок московская область",
        "дрова с доставкой на дом МО частный дом",
        "берёзовые дрова доставка подмосковье коттедж",
    ],
    "Охрана": [
        "охрана коттеджного посёлка МО частная охранная компания",
        "видеонаблюдение коттедж подмосковье установка",
        "сигнализация загородный дом МО монтаж компания",
    ],
    "Строительство и ремонт": [
        "ремонт загородного дома коттедж МО бригада",
        "строительная компания загородные дома московская область",
        "кровля фасад ремонт коттедж подмосковье",
    ],
}

# ─── Контактные страницы ─────────────────────────────────────────────────────

CONTACT_PAGE_SLUGS = [
    "/contacts", "/contact", "/kontakty", "/kontakt",
    "/о-нас", "/o-nas", "/about", "/о-компании",
    "/about-us", "/svyaz", "/svyazi",
]

# ─── Regex-паттерны ──────────────────────────────────────────────────────────

PHONE_RE = re.compile(
    r"(?:\+7|8)[\s\-]?\(?(\d{3})\)?[\s\-]?(\d{3})[\s\-]?(\d{2})[\s\-]?(\d{2})"
)
EMAIL_RE  = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")
VK_RE     = re.compile(r"https?://(?:www\.)?vk\.com/([\w.\-]+)")
TG_RE     = re.compile(r"https?://(?:www\.)?t(?:elegram)?\.me/([\w.\-]+)")
WA_RE     = re.compile(r"https?://(?:wa\.me|api\.whatsapp\.com/send)[/?][\w=&%+]+")

SUBURBS_KW = [
    "подмосковь", "московская область", " мо ", " мо,", "загород",
    "коттедж", "дача", "посёлок", "поселок", "ленинградск",
    "новорижск", "рублёвк", "калужск", "киевск",
]

# ─── Checkpoint ──────────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done_urls": [], "companies": []}


def save_checkpoint(done_urls: set[str], companies: list[dict]):
    data = {
        "done_urls": list(done_urls),
        "companies": companies,
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Вспомогательные функции ─────────────────────────────────────────────────

def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lstrip("www.")
    except Exception:
        return ""


def is_skip_domain(url: str) -> bool:
    domain = get_domain(url)
    return any(d in domain for d in SKIP_DOMAINS)


def get_with_retry(url: str, max_retry: int = MAX_RETRY, timeout: int = HTTP_TIMEOUT) -> requests.Response | None:
    """GET с повторными попытками и экспоненциальным бэкофом."""
    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else 0
            if code in (403, 404, 410):
                return None  # не стоит повторять
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"     ⚠ HTTP {code}, retry {attempt}/{max_retry} через {wait:.1f}с")
            time.sleep(wait)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"     ⚠ Timeout/Connection, retry {attempt}/{max_retry} через {wait:.1f}с")
            time.sleep(wait)
        except Exception as e:
            print(f"     ✗ Fetch error: {e}")
            return None
    return None


# ─── DuckDuckGo поиск ────────────────────────────────────────────────────────

def search_ddg(query: str, limit: int = SEARCH_LIMIT) -> list[dict]:
    """Поиск через DuckDuckGo, возвращает [{url, title, description}]."""
    results = []
    for attempt in range(1, 4):
        try:
            with DDGS() as ddgs:
                for r in ddgs.text(query, region="ru-ru", max_results=limit):
                    url = r.get("href", "")
                    if url and not is_skip_domain(url):
                        results.append({
                            "url":         url,
                            "title":       r.get("title", ""),
                            "description": r.get("body", ""),
                        })
            return results
        except Exception as e:
            wait = 5 * attempt
            print(f"     ⚠ DDG error (попытка {attempt}/3): {e}. Пауза {wait}с")
            time.sleep(wait)
    return results


# ─── Извлечение контактов с сайта ────────────────────────────────────────────

def extract_contacts(soup: BeautifulSoup, base_url: str) -> dict:
    """Из разобранного HTML вытягивает контакты."""
    # Полный текст (без script/style)
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    full_text = soup.get_text(separator=" ")
    all_links = [a.get("href", "") for a in soup.find_all("a", href=True)]

    # Телефон
    phone = ""
    tel_links = [h for h in all_links if str(h).startswith("tel:")]
    if tel_links:
        phone = tel_links[0].replace("tel:", "").strip()
    if not phone:
        m = PHONE_RE.search(full_text)
        if m:
            phone = m.group(0)

    # Email
    email = ""
    mail_links = [h for h in all_links if str(h).startswith("mailto:")]
    if mail_links:
        email = mail_links[0].replace("mailto:", "").split("?")[0].strip()
    if not email:
        m = EMAIL_RE.search(full_text)
        if m:
            email = m.group(0)

    # VK
    vk_url = ""
    for link in all_links:
        m = VK_RE.search(str(link))
        if m and m.group(1) not in ("share", "sharer"):
            vk_url = link
            break

    # Telegram
    telegram = ""
    for link in all_links:
        m = TG_RE.search(str(link))
        if m:
            telegram = link
            break
    if not telegram:
        m = TG_RE.search(full_text)
        if m:
            telegram = m.group(0)

    # WhatsApp
    whatsapp = ""
    for link in all_links:
        if "wa.me" in str(link) or "whatsapp" in str(link).lower():
            whatsapp = link
            break
    if not whatsapp:
        m = WA_RE.search(full_text)
        if m:
            whatsapp = m.group(0)

    # Адрес (schema.org)
    address = ""
    for tag in soup.find_all(attrs={"itemprop": "address"}):
        t = tag.get_text(strip=True)
        if t:
            address = t[:200]
            break

    # Описание
    description = ""
    og_desc = (soup.find("meta", property="og:description")
                or soup.find("meta", attrs={"name": "description"}))
    if og_desc:
        description = og_desc.get("content", "").strip()[:300]

    # Работает в Подмосковье?
    check_text = (description + " " + full_text[:3000]).lower()
    works_in_suburbs = any(k in check_text for k in SUBURBS_KW)

    return {
        "phone":            phone,
        "email":            email,
        "vk_url":           vk_url,
        "telegram":         telegram,
        "whatsapp":         whatsapp,
        "address":          address,
        "description":      description,
        "works_in_suburbs": works_in_suburbs,
    }


def find_contact_page(base_url: str, soup: BeautifulSoup) -> str | None:
    """Ищет ссылку на страницу контактов в навигации."""
    contact_kw = ["контакт", "contact", "о нас", "о компании", "связь"]
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").lower()
        text = a.get_text(strip=True).lower()
        if any(kw in href or kw in text for kw in contact_kw):
            full = urljoin(base_url, a["href"])
            if get_domain(full) == get_domain(base_url):
                return full
    # Пробуем типовые slug'и
    for slug in CONTACT_PAGE_SLUGS:
        return None  # не спамим лишними запросами без веского повода
    return None


def get_company_name(soup: BeautifulSoup) -> str:
    """Определяет название компании из meta/schema."""
    # 1. schema.org
    for tag in soup.find_all(attrs={"itemprop": "name"}):
        t = tag.get_text(strip=True)
        if t and len(t) < 100:
            return t
    # 2. og:site_name
    og = soup.find("meta", property="og:site_name")
    if og:
        val = og.get("content", "").strip()
        if val:
            return val[:100]
    # 3. <title> (берём первый сегмент)
    if soup.title and soup.title.string:
        raw = soup.title.string.strip()
        for sep in [" | ", " — ", " - ", " :: "]:
            if sep in raw:
                return raw.split(sep)[0].strip()[:100]
        return raw[:100]
    return ""


def scrape_company(url: str, category: str) -> dict | None:
    """Загружает сайт, пробует контактную страницу, возвращает карточку."""
    resp = get_with_retry(url)
    if resp is None:
        return None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None

    company_name = get_company_name(soup)
    if not company_name:
        return None

    contacts = extract_contacts(soup, url)

    # Если не нашли телефон — пробуем страницу контактов
    if not contacts["phone"]:
        contact_url = find_contact_page(url, soup)
        if contact_url:
            resp2 = get_with_retry(contact_url)
            if resp2:
                try:
                    soup2 = BeautifulSoup(resp2.text, "html.parser")
                    contacts2 = extract_contacts(soup2, contact_url)
                    # Обновляем только пустые поля
                    for field in ["phone", "email", "address", "telegram", "vk_url", "whatsapp"]:
                        if not contacts[field] and contacts2[field]:
                            contacts[field] = contacts2[field]
                    if not contacts["works_in_suburbs"]:
                        contacts["works_in_suburbs"] = contacts2["works_in_suburbs"]
                except Exception:
                    pass

    return {
        "company_name":     company_name,
        "website":          url,
        "category":         category,
        "source":           "scrape",
        "source_url":       url,
        "rating":           None,
        "reviews_count":    None,
        **contacts,
    }


def company_from_meta(item: dict, category: str) -> dict | None:
    """Карточка из метаданных поиска (fallback без скрапинга)."""
    title = item.get("title", "").strip()
    desc  = item.get("description", "").strip()
    url   = item.get("url", "")
    if not title:
        return None
    phone_m = PHONE_RE.search(desc)
    return {
        "company_name":     title[:100],
        "phone":            phone_m.group(0) if phone_m else "",
        "email":            "",
        "website":          url,
        "vk_url":           "",
        "telegram":         "",
        "whatsapp":         "",
        "rating":           None,
        "reviews_count":    None,
        "address":          "",
        "description":      desc[:300],
        "works_in_suburbs": any(k in (title + desc).lower() for k in SUBURBS_KW),
        "category":         category,
        "source":           "search_meta",
        "source_url":       url,
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
    if row.get("works_in_suburbs"):                                        score += 3
    if (row.get("rating") or 0) >= 4.5:                                   score += 2
    if (row.get("reviews_count") or 0) >= 15:                             score += 2
    if row.get("website") and "http" in str(row.get("website", "")):      score += 1
    if row.get("email"):                                                   score += 1
    if row.get("vk_url") or row.get("telegram") or row.get("whatsapp"):   score += 1
    if row.get("phone"):                                                   score += 1
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

    # Дедупликация по домену (один сайт = одна компания)
    df["_domain"] = df["website"].apply(get_domain)
    df = df.drop_duplicates(subset=["_domain"], keep="first")
    df = df.drop(columns=["_domain"])

    # Дедупликация по имени
    df = df.drop_duplicates(subset=["company_name"], keep="first")

    # Дедупликация по телефону (только непустые)
    if "phone" in df.columns:
        has_phone = df["phone"] != ""
        df = pd.concat([
            df[has_phone].drop_duplicates(subset=["phone"], keep="first"),
            df[~has_phone],
        ], ignore_index=True)

    df["priority"] = df.apply(lambda r: assign_priority(r.to_dict()), axis=1)
    df["status"]          = "Не обработан"
    df["contact_date"]    = ""
    df["manager_comment"] = ""

    cols_order = [
        "priority", "category", "company_name", "phone", "email",
        "website", "vk_url", "telegram", "whatsapp",
        "rating", "reviews_count",
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

def save_results(df: pd.DataFrame, all_raw: list[dict], suffix: str = ""):
    if df.empty:
        print("\n⚠  Нет данных для сохранения.")
        return

    tag = f"_{suffix}" if suffix else ""
    xlsx_path = DATA_DIR / f"companies_v3{tag}_{TODAY}.xlsx"
    csv_path  = DATA_DIR / f"companies_v3{tag}_{TODAY}.csv"
    json_path = DATA_DIR / f"companies_v3_raw{tag}_{TODAY}.json"

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

    df.to_csv(csv_path, index=True, encoding="utf-8-sig")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_raw, f, ensure_ascii=False, indent=2)

    sep = "=" * 60
    print(f"\n{sep}")
    print("  РЕЗУЛЬТАТЫ  (parser_v3)")
    print(sep)
    print(f"  Всего компаний:  {len(df)}")
    for p in ["P1", "P2", "P3"]:
        cnt = len(df[df["priority"] == p])
        print(f"  Приоритет {p}:    {cnt}")
    print()
    print("  По категориям:")
    for cat, count in df["category"].value_counts().items():
        p1 = len(df[(df["category"] == cat) & (df["priority"] == "P1")])
        print(f"    {cat:<28} {count:>4}  (P1: {p1})")
    print()
    print(f"  [Excel]  {xlsx_path}")
    print(f"  [CSV]    {csv_path}")
    print(f"  [JSON]   {json_path}")
    print(sep)


# ─── Главная функция ─────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="PASS24 Market Парсер v3")
    p.add_argument("--resume",    action="store_true", help="Продолжить с checkpoint")
    p.add_argument("--fast",      action="store_true", help="Быстрый режим (меньше URL)")
    p.add_argument("--category",  type=str, default=None, help="Только указанная категория")
    p.add_argument("--no-scrape", action="store_true", help="Только поиск, без скрапинга сайтов")
    return p.parse_args()


def main():
    args = parse_args()

    search_limit = 5 if args.fast else SEARCH_LIMIT

    sep = "=" * 60
    print(sep)
    print("  PASS24 Market — Парсер v3 (DDG + BS4, без платных API)")
    print(f"  Дата: {TODAY}")
    if args.fast:     print("  Режим: FAST")
    if args.resume:   print("  Режим: RESUME (продолжение)")
    if args.category: print(f"  Категория: {args.category}")
    print(sep)

    # Выбор категорий
    queries_to_run: dict[str, list[str]] = {}
    if args.category:
        if args.category not in SEARCH_QUERIES:
            print(f"\n[ОШИБКА] Неизвестная категория: {args.category!r}")
            print("Доступные:", ", ".join(SEARCH_QUERIES))
            return
        queries_to_run = {args.category: SEARCH_QUERIES[args.category]}
    else:
        queries_to_run = SEARCH_QUERIES

    # Checkpoint
    checkpoint = load_checkpoint() if args.resume else {"done_urls": [], "companies": []}
    done_urls:  set[str]  = set(checkpoint["done_urls"])
    all_companies: list[dict] = checkpoint["companies"]

    # ── Этап 1: Поиск URL через DuckDuckGo ───────────────────────────────
    print("\n[1/2] Поиск сайтов через DuckDuckGo...")
    seen_urls:      set[str]                     = set(done_urls)
    candidate_urls: list[tuple[str, str, dict]]  = []  # (url, category, meta)

    for category, queries in queries_to_run.items():
        print(f"\n  Категория: {category}")
        cat_count = 0
        for query in queries:
            print(f"  → {query!r}")
            results = search_ddg(query, limit=search_limit)
            for r in results:
                url = r["url"]
                if url not in seen_urls:
                    seen_urls.add(url)
                    candidate_urls.append((url, category, r))
                    cat_count += 1
            time.sleep(PAUSE_DDG + random.uniform(0, 1))
        print(f"     ✓ Новых URL: {cat_count}")

    if args.resume:
        print(f"\n  Уже обработано ранее: {len(done_urls)}")
    print(f"  Новых для обработки: {len(candidate_urls)}")

    if not candidate_urls:
        print("\n  Нет новых URL. Обрабатываю накопленные данные...")

    # ── Этап 2: Скрапинг ─────────────────────────────────────────────────
    if not args.no_scrape:
        print(f"\n[2/2] Скрапинг сайтов...")
        total = len(candidate_urls)

        for i, (url, category, meta) in enumerate(candidate_urls, 1):
            print(f"  [{i}/{total}] {url[:70]}")

            company = scrape_company(url, category)

            if company:
                all_companies.append(company)
                phone = company.get("phone") or "—"
                wa    = " 📲" if company.get("whatsapp") else ""
                print(f"     ✓ {company['company_name'][:50]} | {phone}{wa}")
            else:
                fallback = company_from_meta(meta, category)
                if fallback:
                    all_companies.append(fallback)
                    print(f"     ~ (meta) {fallback['company_name'][:50]}")
                else:
                    print(f"     ✗ Нет данных")

            done_urls.add(url)
            # Сохраняем checkpoint каждые 10 сайтов
            if i % 10 == 0:
                save_checkpoint(done_urls, all_companies)
                print(f"     💾 Checkpoint сохранён ({len(all_companies)} компаний)")

            time.sleep(PAUSE_SCRAPE + random.uniform(0, 0.8))
    else:
        # Без скрапинга — берём только мета
        print(f"\n[2/2] Режим без скрапинга — только метаданные поиска...")
        for url, category, meta in candidate_urls:
            fallback = company_from_meta(meta, category)
            if fallback:
                all_companies.append(fallback)

    # ── Обработка и сохранение ────────────────────────────────────────────
    save_checkpoint(done_urls, all_companies)

    print(f"\n  Сырых записей: {len(all_companies)}")
    print("  Дедупликация и приоритизация...")
    df = process_companies(all_companies)

    suffix = args.category.replace(" ", "_") if args.category else ""
    save_results(df, all_companies, suffix=suffix)

    # Удаляем checkpoint после успешного завершения
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print(f"\n  Checkpoint удалён (парсинг завершён успешно).")


if __name__ == "__main__":
    main()
