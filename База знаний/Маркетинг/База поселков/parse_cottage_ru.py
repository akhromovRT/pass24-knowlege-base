#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для парсинга информации о коттеджных поселках РФ
с сайта Cottage.ru (https://www.cottage.ru/objects/village/)

Результат сохраняется в файл "Результат парсинга информации в интернете.csv"
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging
from urllib.parse import urljoin
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parsing_cottage_ru.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

DELAY_MIN = 3
DELAY_MAX = 7
DELAY_429 = 25
MAX_SELENIUM_PHONE_ATTEMPTS = 60
PAGE_LOAD_TIMEOUT = 20

FIELDS = [
    'ID', 'Название поселка', 'Регион', 'Город/Район', 'Адрес', 'Координаты', 'Ссылка на источник',
    'Количество домов/участков', 'Статус поселка', 'Наличие ограждения', 'Наличие КПП',
    'Количество КПП', 'Наличие охраны', 'Тип охраны', 'Наличие интернета',
    'Тип управления', 'Название УК/ТСЖ', 'ФИО председателя/руководителя', 'Телефон основной',
    'Телефон дополнительный', 'Email', 'Сайт поселка/УК', 'Социальные сети',
    'Challenges (Проблемы)', 'Authority (Полномочия)', 'Money (Бюджет)', 'Priority (Приоритет)',
    'Оценка целевого клиента',
    'Статус в CRM', 'ID сделки в CRM', 'Менеджер', 'Дата первого контакта',
    'Дата последнего контакта', 'Следующий контакт', 'Комментарии',
    'Результат', 'Дата продажи', 'Сумма сделки', 'Причина отказа',
    'Источник информации', 'Дата добавления в базу', 'Дата последнего обновления', 'Кто добавил'
]

BASE_COTTAGE = 'https://www.cottage.ru'


class SeleniumPhoneExtractor:
    """Извлечение телефонов с помощью Selenium"""

    def __init__(self, headless: bool = True):
        self.driver = None
        self.headless = headless
        self._init_driver()

    def _init_driver(self):
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            logger.info("Selenium WebDriver инициализирован успешно")
        except ImportError as e:
            logger.error(f"Selenium не установлен: {e}")
            self.driver = None
        except Exception as e:
            logger.error(f"Ошибка инициализации Selenium: {e}")
            self.driver = None

    def extract_phone_from_url(self, url: str) -> Optional[str]:
        if not self.driver or not url:
            return None
        try:
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            phone = None
            tel_links = self.driver.find_elements(By.XPATH, "//a[starts-with(@href, 'tel:')]")
            for link in tel_links:
                href = link.get_attribute('href')
                if href:
                    phone = href.replace('tel:', '').strip()
                    if phone:
                        break
            if not phone:
                elems = self.driver.find_elements(By.XPATH, "//*[@data-phone or @data-tel or @data-telephone]")
                for e in elems:
                    phone = e.get_attribute('data-phone') or e.get_attribute('data-tel') or e.get_attribute('data-telephone')
                    if phone:
                        break
            if not phone:
                phone = self._extract_phone_from_text(self.driver.page_source)
            if phone:
                phone = self._normalize_phone(phone)
                return phone
            return None
        except (TimeoutException, WebDriverException, Exception) as e:
            logger.debug(f"Ошибка извлечения телефона с {url}: {e}")
            return None

    def _extract_phone_from_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        for pat in [r'\+?7\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}', r'\+?7\s?\d{10}', r'8\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}', r'\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2}']:
            m = re.search(pat, text)
            if m:
                return m.group(0).strip()
        return None

    def _normalize_phone(self, phone: str) -> str:
        if not phone:
            return phone
        phone = re.sub(r'[^\d+]', '', phone)
        if phone.startswith('8'):
            phone = '+7' + phone[1:]
        elif not phone.startswith('+7'):
            if phone.startswith('7'):
                phone = '+' + phone
            else:
                phone = '+7' + phone
        return phone

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

    def __del__(self):
        self.close()


class CottageRuParser:
    """Парсинг коттеджных поселков с Cottage.ru /objects/village/"""

    def __init__(self, use_selenium: bool = True, selenium_headless: bool = True):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.results: List[Dict] = []
        self.village_id = 1
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.use_selenium = use_selenium
        self.selenium_extractor = None
        self.selenium_phones_count = 0
        self.selenium_phone_attempts = 0
        if use_selenium:
            try:
                self.selenium_extractor = SeleniumPhoneExtractor(headless=selenium_headless)
            except Exception as e:
                logger.warning(f"Selenium не инициализирован: {e}")
                self.use_selenium = False

    def delay(self):
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    def _can_do_selenium_phone(self) -> bool:
        if self.selenium_phone_attempts >= MAX_SELENIUM_PHONE_ATTEMPTS:
            return False
        self.selenium_phone_attempts += 1
        return True

    def extract_phone(self, text: str) -> Optional[str]:
        if not text:
            return None
        # Пропускаем замаскированные (x-xx)
        if 'x-xx' in text or re.search(r'\d-[xх]\d', text, re.I):
            return None
        for pat in [r'\+?7\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}', r'\+?7\s?\d{10}', r'8\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}', r'\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2}']:
            m = re.search(pat, text)
            if m:
                p = m.group(0).strip()
                p = re.sub(r'[\s\-\(\)]', '', p)
                if p.startswith('8'):
                    p = '+7' + p[1:]
                elif not p.startswith('+7'):
                    p = '+7' + p
                return p
        return None

    def validate_village_name(self, name: str) -> bool:
        if not name or len(name.strip()) < 3:
            return False
        invalid = [
            r'подробнее', r'с\s*коммуникациями', r'в\s*избранное', r'ещё\s*фото', r'продажа', r'фото',
            r'смотреть', r'клик', r'нажмите', r'^\d+$', r'^[а-яё]{1,2}$',
            r'коттеджные\s*посёл?ки\s*в\s*', r'поселки\s*в\s*области', r'коттеджные\s*поселки$',
        ]
        nl = name.lower().strip()
        for p in invalid:
            if re.search(p, nl, re.I):
                return False
        if len(name.strip()) < 5:
            return False
        if nl in ['коттеджные посёлки', 'коттеджные поселки', 'поселки', 'кп']:
            return False
        return True

    def _extract_city_or_district(self, block) -> Optional[str]:
        txt = block.get_text() if hasattr(block, 'get_text') else ''
        # город X, X район, Y шоссе
        m = re.search(r'город\s+([А-Яа-яёЁ\-\s]+?)(?:\s|,|$|\.)', txt)
        if m:
            return ('город ' + m.group(1)).strip()
        m = re.search(r'([А-Яа-яёЁ\-\s]+?(?:район|г\.|гор\.))', txt, re.I)
        if m:
            return m.group(1).strip()
        m = re.search(r'([А-Яа-яёЁ\-\s]+?шоссе)(?:\s*,?\s*\d+\s*км)?', txt, re.I)
        if m:
            return m.group(1).strip()
        # Ссылки с location= или direction=
        a = block.find('a', href=re.compile(r'location=|direction='))
        if a and a.get_text(strip=True):
            return a.get_text(strip=True)
        return None

    def _parse_village_card(self, block, detail_href: str) -> Optional[Dict]:
        try:
            village = self._create_empty_village()
            href = detail_href
            if href.startswith('/'):
                full_url = urljoin(BASE_COTTAGE, href)
            else:
                full_url = href if href.startswith('http') else urljoin(BASE_COTTAGE, '/' + href.lstrip('/'))
            village['Ссылка на источник'] = full_url

            link = block.find('a', href=re.compile(r'/objects/village/[^/]+\.html')) if hasattr(block, 'find') else None
            if not link:
                link = block if getattr(block, 'name', None) == 'a' else None

            name = None
            if link:
                name = link.get_text(strip=True) if hasattr(link, 'get_text') else None
            if not name:
                for tag in ['h2', 'h3', 'h4']:
                    h = block.find(tag) if hasattr(block, 'find') else None
                    if h and h.get_text(strip=True) and self.validate_village_name(h.get_text(strip=True)):
                        name = h.get_text(strip=True)
                        break
            if not name and hasattr(block, 'get_text'):
                for line in (block.get_text() or '').splitlines():
                    line = line.strip()
                    if line and self.validate_village_name(line) and 'подробнее' not in line.lower() and 'с коммуникациями' not in line.lower():
                        name = line
                        break

            if not name or not self.validate_village_name(name):
                return None
            village['Название поселка'] = name

            village['Регион'] = 'Московская область'
            city = self._extract_city_or_district(block)
            if city:
                village['Город/Район'] = city

            block_text = block.get_text() if hasattr(block, 'get_text') else ''
            phone = self.extract_phone(block_text)
            if phone:
                village['Телефон основной'] = phone

            if not village.get('Телефон основной') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                self.delay()
                ph = self.selenium_extractor.extract_phone_from_url(full_url)
                if ph:
                    village['Телефон основной'] = ph
                    self.selenium_phones_count += 1

            self.village_id += 1
            return village
        except Exception as e:
            logger.debug(f"Ошибка разбора карточки: {e}")
            return None

    def _create_empty_village(self) -> Dict:
        return {
            'ID': self.village_id,
            'Название поселка': None,
            'Регион': 'Московская область',
            'Город/Район': None,
            'Адрес': None,
            'Координаты': None,
            'Ссылка на источник': None,
            'Количество домов/участков': None,
            'Статус поселка': None,
            'Наличие ограждения': None,
            'Наличие КПП': None,
            'Количество КПП': None,
            'Наличие охраны': None,
            'Тип охраны': None,
            'Наличие интернета': None,
            'Тип управления': None,
            'Название УК/ТСЖ': None,
            'ФИО председателя/руководителя': None,
            'Телефон основной': None,
            'Телефон дополнительный': None,
            'Email': None,
            'Сайт поселка/УК': None,
            'Социальные сети': None,
            'Challenges (Проблемы)': None,
            'Authority (Полномочия)': 'Неизвестно',
            'Money (Бюджет)': 'Неизвестно',
            'Priority (Приоритет)': 'Неизвестно',
            'Оценка целевого клиента': 'Требует проверки',
            'Статус в CRM': 'Не обработан',
            'ID сделки в CRM': None,
            'Менеджер': None,
            'Дата первого контакта': None,
            'Дата последнего контакта': None,
            'Следующий контакт': None,
            'Комментарии': None,
            'Результат': 'Не обработан',
            'Дата продажи': None,
            'Сумма сделки': None,
            'Причина отказа': None,
            'Источник информации': 'Cottage.ru',
            'Дата добавления в базу': self.current_date,
            'Дата последнего обновления': self.current_date,
            'Кто добавил': 'Парсер'
        }

    def parse_cottage_ru(self, base_url: str = 'https://www.cottage.ru/objects/village/', max_pages: int = 5) -> List[Dict]:
        logger.info("Начинаем парсинг Cottage.ru: %s", base_url)
        villages = []
        seen_hrefs = set()

        for page in range(1, max_pages + 1):
            self.delay()
            url = base_url if page == 1 else f"{base_url.rstrip('/')}?page={page}"

            for attempt in range(2):
                try:
                    resp = self.session.get(url, timeout=60)
                    if resp.status_code == 429:
                        logger.warning("Получен 429, пауза %s с", DELAY_429)
                        time.sleep(DELAY_429)
                        continue
                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt < 1:
                        time.sleep(DELAY_429)
                        continue
                    logger.warning("Ошибка загрузки %s: %s", url, e)
                    if page > 1:
                        return villages
                    return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            links = soup.find_all('a', href=re.compile(r'/objects/village/[^/]+\.html'))
            if not links and page > 1:
                break

            for link in links:
                href = link.get('href', '')
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                parent = link.find_parent(['div', 'article', 'section'])
                if not parent:
                    parent = link
                v = self._parse_village_card(parent, href)
                if v and self.validate_village_name(v.get('Название поселка', '')):
                    villages.append(v)
                    logger.info("Добавлен поселок: %s", v.get('Название поселка'))

            logger.info("Страница %s/%s: найдено поселков всего: %s", page, max_pages, len(villages))

        logger.info("Всего поселков с Cottage.ru: %s", len(villages))
        return villages

    def save_to_csv(self, filename: str = 'Результат парсинга информации в интернете.csv'):
        if not self.results:
            logger.warning("Нет данных для сохранения")
            return
        existing_data = []
        existing_ids = set()
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8-sig', newline='') as f:
                    for row in csv.DictReader(f):
                        existing_data.append(row)
                        if row.get('ID'):
                            try:
                                existing_ids.add(int(row['ID']))
                            except ValueError:
                                pass
            except Exception as e:
                logger.warning("Ошибка чтения CSV: %s", e)
        max_id = max(existing_ids) if existing_ids else 0
        for v in self.results:
            if v.get('ID'):
                try:
                    if int(v['ID']) <= max_id:
                        max_id += 1
                        v['ID'] = max_id
                except ValueError:
                    max_id += 1
                    v['ID'] = max_id
            else:
                max_id += 1
                v['ID'] = max_id
        all_data = existing_data + self.results
        try:
            with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                w = csv.DictWriter(f, fieldnames=FIELDS)
                w.writeheader()
                w.writerows(all_data)
            logger.info("Сохранено %s записей в %s, всего: %s", len(self.results), filename, len(all_data))
        except Exception as e:
            logger.error("Ошибка сохранения CSV: %s", e)

    def close(self):
        if self.selenium_extractor:
            self.selenium_extractor.close()


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Парсинг коттеджных поселков с Cottage.ru')
    ap.add_argument('--no-selenium', action='store_true', help='Не использовать Selenium')
    ap.add_argument('--selenium-headless', action='store_true', default=True, help='Selenium в headless')
    ap.add_argument('--output', default='Результат парсинга информации в интернете.csv', help='Выходной CSV')
    ap.add_argument('--max-pages', type=int, default=5, help='Макс. страниц')
    ap.add_argument('--url', default='https://www.cottage.ru/objects/village/', help='URL каталога поселков')
    args = ap.parse_args()

    p = CottageRuParser(use_selenium=not args.no_selenium, selenium_headless=args.selenium_headless)
    try:
        villages = p.parse_cottage_ru(base_url=args.url, max_pages=args.max_pages)
        p.results = villages
        p.save_to_csv(args.output)
        logger.info("Парсинг завершен. Найдено поселков: %s", len(villages))
        if p.selenium_phones_count > 0:
            logger.info("Телефонов через Selenium: %s", p.selenium_phones_count)
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.error("Критическая ошибка: %s", e)
    finally:
        p.close()


if __name__ == '__main__':
    main()
