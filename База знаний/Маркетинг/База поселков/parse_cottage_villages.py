#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для парсинга информации о коттеджных поселках РФ
из различных источников в интернете.

Источники:
- Cian.ru
- DomClick.ru
- Яндекс.Недвижимость
- Cottage.ru
- Poselki.ru
- Авито Недвижимость

Результат сохраняется в файл "Результат парсинга информации в интернете.csv"
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import re
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging
from urllib.parse import urljoin, urlparse, parse_qs
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parsing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Заголовки для HTTP запросов (имитация браузера)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Задержка между запросами (секунды)
DELAY_MIN = 2
DELAY_MAX = 5

# Лимиты для предотвращения зависаний
MAX_SELENIUM_PHONE_ATTEMPTS = 60   # макс. попыток получения телефона через Selenium за один запуск
MAX_ENRICH = 40                    # макс. записей для обогащения из детальных страниц
PAGE_LOAD_TIMEOUT = 20             # таймаут загрузки страницы в Selenium (сек)

# Структура полей таблицы согласно инструкции
FIELDS = [
    # Блок 1: Основная информация о поселке
    'ID', 'Название поселка', 'Регион', 'Город/Район', 'Адрес', 'Координаты', 'Ссылка на источник',
    # Блок 2: Характеристики поселка
    'Количество домов/участков', 'Статус поселка', 'Наличие ограждения', 'Наличие КПП',
    'Количество КПП', 'Наличие охраны', 'Тип охраны', 'Наличие интернета',
    # Блок 3: Контактная информация
    'Тип управления', 'Название УК/ТСЖ', 'ФИО председателя/руководителя', 'Телефон основной',
    'Телефон дополнительный', 'Email', 'Сайт поселка/УК', 'Социальные сети',
    # Блок 4: Квалификация по CHAMP
    'Challenges (Проблемы)', 'Authority (Полномочия)', 'Money (Бюджет)', 'Priority (Приоритет)',
    'Оценка целевого клиента',
    # Блок 5: Статус работы
    'Статус в CRM', 'ID сделки в CRM', 'Менеджер', 'Дата первого контакта',
    'Дата последнего контакта', 'Следующий контакт', 'Комментарии',
    # Блок 6: Результаты работы
    'Результат', 'Дата продажи', 'Сумма сделки', 'Причина отказа',
    # Блок 7: Дополнительная информация
    'Источник информации', 'Дата добавления в базу', 'Дата последнего обновления', 'Кто добавил'
]


class SeleniumPhoneExtractor:
    """Класс для извлечения телефонов с использованием Selenium"""
    
    def __init__(self, headless: bool = True):
        """
        Инициализация Selenium WebDriver
        
        Args:
            headless: Запуск браузера в фоновом режиме (без GUI)
        """
        self.driver = None
        self.headless = headless
        self._init_driver()
    
    def _init_driver(self):
        """Инициализация Chrome WebDriver"""
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
            logger.error(f"Selenium не установлен. Установите: pip install selenium webdriver-manager. Ошибка: {e}")
            self.driver = None
        except Exception as e:
            logger.error(f"Ошибка инициализации Selenium WebDriver: {e}")
            logger.info("Парсинг будет продолжен без Selenium. Для получения телефонов используйте --no-selenium")
            self.driver = None
    
    def extract_phone_from_url(self, url: str, wait_time: int = 5) -> Optional[str]:
        """
        Извлечение телефона со страницы с использованием Selenium
        
        Args:
            url: URL страницы для парсинга
            wait_time: Время ожидания загрузки страницы (секунды)
        
        Returns:
            Номер телефона или None
        """
        if not self.driver or not url:
            return None
        
        try:
            logger.debug(f"Открываем страницу с Selenium: {url}")
            self.driver.get(url)
            
            # Ждем загрузки страницы
            time.sleep(random.uniform(2, 4))  # Случайная задержка для имитации человеческого поведения
            
            # Паттерны для поиска кнопок "Показать телефон"
            show_phone_button_selectors = [
                # Cian.ru
                "//button[contains(@class, 'phone') or contains(@class, 'show-phone') or contains(text(), 'Показать телефон')]",
                "//a[contains(@class, 'phone') or contains(@class, 'show-phone')]",
                "//*[@data-name='PhoneButton']",
                "//*[contains(@data-testid, 'phone')]",
                # Общие паттерны
                "//button[contains(translate(text(), 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ', 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'), 'показать телефон')]",
                "//a[contains(translate(text(), 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ', 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'), 'показать телефон')]",
                "//*[contains(@class, 'show-phone')]",
                "//*[contains(@class, 'phone-button')]",
                "//*[contains(@class, 'contact-phone')]",
            ]
            
            # Пытаемся найти и кликнуть на кнопку "Показать телефон"
            phone_button = None
            for selector in show_phone_button_selectors:
                try:
                    if selector.startswith('//'):
                        # XPath селектор
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        # CSS селектор
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            phone_button = element
                            break
                    
                    if phone_button:
                        break
                except Exception as e:
                    logger.debug(f"Не удалось найти элемент по селектору {selector}: {e}")
                    continue
            
            # Кликаем на кнопку, если нашли
            if phone_button:
                try:
                    # Прокручиваем к элементу
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", phone_button)
                    time.sleep(0.5)
                    
                    # Пытаемся кликнуть
                    phone_button.click()
                    time.sleep(1)  # Ждем загрузки телефона
                    logger.debug("Клик на кнопку 'Показать телефон' выполнен")
                except Exception as e:
                    logger.debug(f"Не удалось кликнуть на кнопку: {e}")
                    # Пробуем через JavaScript
                    try:
                        self.driver.execute_script("arguments[0].click();", phone_button)
                        time.sleep(1)
                    except:
                        pass
            
            # Ищем телефон в различных местах на странице
            phone = None
            
            # Стратегия 1: Поиск в tel: ссылках
            tel_links = self.driver.find_elements(By.XPATH, "//a[starts-with(@href, 'tel:')]")
            for link in tel_links:
                href = link.get_attribute('href')
                if href:
                    phone = href.replace('tel:', '').strip()
                    if phone:
                        break
            
            # Стратегия 2: Поиск в data-атрибутах
            if not phone:
                phone_elements = self.driver.find_elements(By.XPATH, "//*[@data-phone or @data-tel or @data-telephone]")
                for elem in phone_elements:
                    phone = elem.get_attribute('data-phone') or elem.get_attribute('data-tel') or elem.get_attribute('data-telephone')
                    if phone:
                        break
            
            # Стратегия 3: Поиск в тексте страницы после клика
            if not phone:
                page_source = self.driver.page_source
                phone = self._extract_phone_from_text(page_source)
            
            # Стратегия 4: Поиск в видимом тексте элементов с классом phone
            if not phone:
                phone_elements = self.driver.find_elements(By.CSS_SELECTOR, ".phone, .tel, .telephone, [class*='phone'], [class*='tel']")
                for elem in phone_elements:
                    text = elem.text.strip()
                    if text:
                        phone = self._extract_phone_from_text(text)
                        if phone:
                            break
            
            if phone:
                # Нормализация телефона
                phone = self._normalize_phone(phone)
                logger.info(f"Найден телефон: {phone}")
                return phone
            
            logger.debug("Телефон не найден на странице")
            return None
            
        except TimeoutException:
            logger.warning(f"Таймаут при загрузке страницы: {url}")
            return None
        except WebDriverException as e:
            logger.warning(f"Ошибка WebDriver при парсинге {url}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Ошибка при извлечении телефона с {url}: {e}")
            return None
    
    def _extract_phone_from_text(self, text: str) -> Optional[str]:
        """Извлечение телефона из текста с помощью регулярных выражений"""
        if not text:
            return None
        
        patterns = [
            r'\+?7\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}',
            r'\+?7\s?\d{10}',
            r'8\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}',
            r'\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2}',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _normalize_phone(self, phone: str) -> str:
        """Нормализация формата телефона"""
        if not phone:
            return phone
        
        # Удаляем все символы кроме цифр и +
        phone = re.sub(r'[^\d+]', '', phone)
        
        # Заменяем 8 на +7
        if phone.startswith('8'):
            phone = '+7' + phone[1:]
        elif not phone.startswith('+7'):
            if phone.startswith('7'):
                phone = '+' + phone
            else:
                phone = '+7' + phone
        
        return phone
    
    def close(self):
        """Закрытие WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Selenium WebDriver закрыт")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии WebDriver: {e}")
    
    def __del__(self):
        """Деструктор для автоматического закрытия WebDriver"""
        self.close()


class VillageParser:
    """Класс для парсинга информации о коттеджных поселках"""
    
    def __init__(self, use_selenium: bool = True, selenium_headless: bool = True):
        """
        Инициализация парсера
        
        Args:
            use_selenium: Использовать Selenium для получения телефонов
            selenium_headless: Запуск Selenium в headless режиме
        """
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.results: List[Dict] = []
        self.village_id = 1
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.use_selenium = use_selenium
        self.selenium_extractor = None
        self.selenium_phones_count = 0       # Счетчик телефонов, полученных через Selenium
        self.selenium_phone_attempts = 0     # Счетчик попыток (лимит для защиты от зависаний)
        
        if self.use_selenium:
            try:
                self.selenium_extractor = SeleniumPhoneExtractor(headless=selenium_headless)
            except Exception as e:
                logger.warning(f"Не удалось инициализировать Selenium: {e}. Парсинг будет работать без Selenium.")
                self.use_selenium = False
        
    def delay(self):
        """Случайная задержка между запросами"""
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    def _can_do_selenium_phone(self) -> bool:
        """Проверка лимита попыток получения телефона через Selenium (защита от зависаний)."""
        if self.selenium_phone_attempts >= MAX_SELENIUM_PHONE_ATTEMPTS:
            logger.info(f"Достигнут лимит попыток Selenium ({MAX_SELENIUM_PHONE_ATTEMPTS}). Пропуск получения телефонов.")
            return False
        self.selenium_phone_attempts += 1
        return True
    
    def extract_phone(self, text: str) -> Optional[str]:
        """Извлечение телефона из текста"""
        if not text:
            return None
        
        # Паттерны для поиска телефонов
        patterns = [
            r'\+?7\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}',
            r'\+?7\s?\d{10}',
            r'8\s?\(?\d{3}\)?\s?\d{3}[- ]?\d{2}[- ]?\d{2}',
            r'\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2}',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                phone = match.group(0).strip()
                # Нормализация телефона
                phone = re.sub(r'[\s\-\(\)]', '', phone)
                if phone.startswith('8'):
                    phone = '+7' + phone[1:]
                elif not phone.startswith('+7'):
                    phone = '+7' + phone
                return phone
        return None
    
    def extract_email(self, text: str) -> Optional[str]:
        """Извлечение email из текста"""
        if not text:
            return None
        
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(pattern, text)
        return match.group(0) if match else None
    
    def extract_number(self, text: str) -> Optional[int]:
        """Извлечение числа из текста"""
        if not text:
            return None
        
        # Удаляем пробелы и ищем числа
        numbers = re.findall(r'\d+', text.replace(' ', ''))
        if numbers:
            try:
                return int(numbers[0])
            except ValueError:
                return None
        return None
    
    def validate_village_name(self, name: str) -> bool:
        """Валидация названия поселка"""
        if not name or len(name.strip()) < 3:
            return False
        
        # Список недопустимых паттернов в названиях
        invalid_patterns = [
            r'ещё\s*фото',
            r'первичная\s*продажа',
            r'продажа',
            r'фото',
            r'подробнее',
            r'читать\s*далее',
            r'смотреть',
            r'клик',
            r'нажмите',
            r'подробности',
            r'^\d+$',  # Только цифры
            r'^[а-яё]{1,2}$',  # Одна-две буквы
            r'коттеджные\s*посёлки\s*в\s*',  # Слишком общие названия
            r'коттеджные\s*поселки\s*в\s*',
            r'поселки\s*в\s*области',
            r'коттеджные\s*поселки$',
        ]
        
        name_lower = name.lower().strip()
        
        for pattern in invalid_patterns:
            if re.search(pattern, name_lower, re.I):
                return False
        
        # Проверка на минимальную длину осмысленного названия
        if len(name.strip()) < 5:
            return False
        
        # Исключаем слишком общие названия (должно быть конкретное название)
        if name_lower in ['коттеджные посёлки', 'коттеджные поселки', 'поселки', 'кп']:
            return False
        
        return True
    
    def is_village_url(self, url: str) -> bool:
        """Проверка, является ли URL ссылкой на коттеджный поселок"""
        if not url:
            return False
        
        # Исключаем ссылки на квартиры и другие объекты
        invalid_patterns = [
            r'/sale/flat/',  # Квартиры
            r'/sale/room/',  # Комнаты
            r'/sale/commercial/',  # Коммерческая недвижимость
            r'/sale/garage/',  # Гаражи
        ]
        
        for pattern in invalid_patterns:
            if re.search(pattern, url, re.I):
                return False
        
        # Проверяем наличие ключевых слов для поселков
        village_keywords = [
            r'kottedzhnye-poselki',
            r'poselok',
            r'uchastok',
            r'cottage',
        ]
        
        for keyword in village_keywords:
            if re.search(keyword, url, re.I):
                return True
        
        return True  # По умолчанию считаем валидным, если нет явных признаков квартиры
    
    def parse_cian(self, region: str = 'moskovskaya-oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг Cian.ru"""
        logger.info(f"Начинаем парсинг Cian.ru для региона: {region}")
        villages = []
        
        try:
            # Используем правильный URL для коттеджных поселков
            # Формат URL может быть разным, пробуем несколько вариантов
            base_urls = [
                f"https://www.cian.ru/kottedzhnye-poselki-{region}/",
                f"https://www.cian.ru/cat.php?deal_type=sale&object_type%5B0%5D=2&region={region}",
            ]
            base_url = base_urls[0]  # Используем первый вариант
            
            for page in range(1, max_pages + 1):
                self.delay()
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?p={page}"
                
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Улучшенный поиск карточек поселков
                    # Используем несколько стратегий поиска
                    cards = []
                    
                    # Стратегия 1: Поиск по data-атрибутам
                    cards.extend(soup.find_all(attrs={'data-name': re.compile(r'Card|Offer', re.I)}))
                    
                    # Стратегия 2: Поиск по классам с ключевыми словами
                    cards.extend(soup.find_all(['article', 'div'], class_=re.compile(r'card|item|village|offer|lot', re.I)))
                    
                    # Стратегия 3: Поиск ссылок на поселки
                    links = soup.find_all('a', href=re.compile(r'kottedzhnye-poselki|poselok|uchastok', re.I))
                    for link in links:
                        parent = link.find_parent(['article', 'div'])
                        if parent and parent not in cards:
                            cards.append(parent)
                    
                    # Удаляем дубликаты
                    seen = set()
                    unique_cards = []
                    for card in cards:
                        card_id = id(card)
                        if card_id not in seen:
                            seen.add(card_id)
                            unique_cards.append(card)
                    
                    if not unique_cards:
                        logger.warning(f"Не найдено карточек на странице {page}, пробуем альтернативный метод...")
                        # Альтернативный метод: поиск всех ссылок на поселки
                        all_links = soup.find_all('a', href=re.compile(r'kottedzhnye-poselki', re.I))
                        if not all_links:
                            logger.warning(f"Не найдено ссылок на поселки на странице {page}")
                            break
                    
                    valid_count = 0
                    for card in unique_cards:
                        village_data = self._parse_cian_card(card, url)
                        if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                            villages.append(village_data)
                            valid_count += 1
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено карточек: {len(unique_cards)}, валидных поселков: {valid_count}, всего: {len(villages)}")
                    
                    # Если на странице нет валидных данных, прекращаем парсинг
                    if valid_count == 0 and page > 1:
                        logger.info(f"На странице {page} нет валидных данных, прекращаем парсинг")
                        break
                    
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} Cian.ru: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге Cian.ru: {e}")
        
        logger.info(f"Всего найдено валидных поселков на Cian.ru: {len(villages)}")
        return villages
    
    def _parse_cian_card(self, card, source_url: str) -> Optional[Dict]:
        """Парсинг карточки поселка с Cian.ru"""
        try:
            village = {
                'ID': self.village_id,
                'Название поселка': None,
                'Регион': None,
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
                'Источник информации': 'Cian.ru',
                'Дата добавления в базу': self.current_date,
                'Дата последнего обновления': self.current_date,
                'Кто добавил': 'Парсер'
            }
            
            card_text = card.get_text()
            
            # Поиск ссылки на страницу поселка (приоритет ссылкам на поселки)
            links = card.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                if 'kottedzhnye-poselki' in href or 'poselok' in href or 'uchastok' in href:
                    if href.startswith('/'):
                        href = urljoin('https://www.cian.ru', href)
                    elif not href.startswith('http'):
                        href = urljoin('https://www.cian.ru', '/' + href)
                    
                    # Проверяем, что это не ссылка на квартиру
                    if self.is_village_url(href):
                        village['Ссылка на источник'] = href
                        break
            
            # Если не нашли ссылку на поселок, берем первую валидную ссылку
            if not village['Ссылка на источник'] and links:
                href = links[0].get('href', '')
                if href.startswith('/'):
                    href = urljoin('https://www.cian.ru', href)
                elif not href.startswith('http'):
                    href = urljoin('https://www.cian.ru', '/' + href)
                
                if self.is_village_url(href):
                    village['Ссылка на источник'] = href
            
            # Улучшенный поиск названия
            # Стратегия 1: Поиск по data-атрибутам
            title_elem = card.find(attrs={'data-name': re.compile(r'Title|Name', re.I)})
            
            # Стратегия 2: Поиск заголовков
            if not title_elem:
                title_elem = card.find(['h1', 'h2', 'h3', 'h4'], class_=re.compile(r'title|name|heading', re.I))
            
            # Стратегия 3: Поиск в ссылках на поселки
            if not title_elem:
                for link in links:
                    if 'kottedzhnye-poselki' in link.get('href', '') or 'poselok' in link.get('href', ''):
                        title_text = link.get_text(strip=True)
                        if self.validate_village_name(title_text):
                            title_elem = link
                            break
            
            # Стратегия 4: Поиск первого значимого текста
            if not title_elem:
                # Ищем текст, который похож на название поселка
                text_elements = card.find_all(['a', 'span', 'div'], string=re.compile(r'[А-ЯЁ][а-яё]+.*поселок|поселок.*[А-ЯЁ]', re.I))
                if text_elements:
                    for elem in text_elements:
                        text = elem.get_text(strip=True)
                        if self.validate_village_name(text) and len(text) > 5:
                            title_elem = elem
                            break
            
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                # Очистка названия от лишних символов
                title_text = re.sub(r'\s+', ' ', title_text).strip()
                village['Название поселка'] = title_text
            
            # Улучшенный поиск адреса
            # Стратегия 1: Поиск по data-атрибутам
            address_elem = card.find(attrs={'data-name': re.compile(r'Address|Location|Geo', re.I)})
            
            # Стратегия 2: Поиск по классам
            if not address_elem:
                address_elem = card.find(['div', 'span'], class_=re.compile(r'address|location|geo|region', re.I))
            
            # Стратегия 3: Поиск по тексту с ключевыми словами
            if not address_elem:
                address_patterns = [
                    r'[А-ЯЁ][а-яё]+\s*(?:область|край|район)',
                    r'[А-ЯЁ][а-яё]+\s*шоссе',
                    r'\d+\s*км\s*до\s*МКАД',
                ]
                for pattern in address_patterns:
                    matches = card.find_all(string=re.compile(pattern, re.I))
                    if matches:
                        address_elem = matches[0].parent if hasattr(matches[0], 'parent') else None
                        break
            
            if address_elem:
                address_text = address_elem.get_text(strip=True)
                village['Адрес'] = address_text
                
                # Извлечение региона из адреса
                if 'область' in address_text.lower() or 'край' in address_text.lower():
                    parts = re.split(r'[,;]', address_text)
                    for part in parts:
                        part = part.strip()
                        if 'область' in part.lower() or 'край' in part.lower():
                            village['Регион'] = part
                        elif part and not village.get('Город/Район'):
                            village['Город/Район'] = part
            
            # Улучшенный поиск количества домов/участков
            # Ищем паттерны типа "120 домов", "50 участков", "коттеджей: 80"
            houses_patterns = [
                r'(\d+)\s*(?:дом|участк|коттедж|лот)',
                r'(?:дом|участк|коттедж|лот)[а-яё]*[:\s]+(\d+)',
                r'(\d+)\s*в\s*продаже',
            ]
            
            for pattern in houses_patterns:
                matches = re.findall(pattern, card_text, re.I)
                if matches:
                    try:
                        houses_num = int(matches[0])
                        if 10 <= houses_num <= 10000:  # Разумные пределы
                            village['Количество домов/участков'] = houses_num
                            break
                    except (ValueError, IndexError):
                        continue
            
            # Поиск информации о статусе поселка
            status_keywords = {
                'построен': 'Построен и заселен',
                'заселен': 'Построен и заселен',
                'сдача': 'В строительстве (>80%)',
                'строительств': 'В строительстве (>80%)',
            }
            
            for keyword, status in status_keywords.items():
                if re.search(keyword, card_text, re.I):
                    village['Статус поселка'] = status
                    break
            
            # Поиск информации об инфраструктуре
            if re.search(r'огражден|забор|периметр', card_text, re.I):
                village['Наличие ограждения'] = 'Да'
            
            if re.search(r'кпп|контрольно-пропускной|пропускной\s*пункт', card_text, re.I):
                village['Наличие КПП'] = 'Да'
                # Попытка найти количество КПП
                kpp_match = re.search(r'(\d+)\s*кпп', card_text, re.I)
                if kpp_match:
                    village['Количество КПП'] = int(kpp_match.group(1))
            
            if re.search(r'охрана|чоп|охраняем', card_text, re.I):
                village['Наличие охраны'] = 'Да'
                if 'чоп' in card_text.lower():
                    village['Тип охраны'] = 'ЧОП'
            
            # Поиск телефона
            # Сначала пробуем стандартные методы парсинга
            phone_elem = card.find(['a', 'span', 'button'], href=re.compile(r'tel:', re.I))
            if not phone_elem:
                phone_elem = card.find(['a', 'span', 'button'], class_=re.compile(r'phone|tel|show.*phone', re.I))
            
            if phone_elem:
                href = phone_elem.get('href', '')
                if href:
                    phone = href.replace('tel:', '').strip()
                    village['Телефон основной'] = phone
                else:
                    # Пробуем извлечь из data-атрибутов
                    phone = phone_elem.get('data-phone') or phone_elem.get('data-tel')
                    if phone:
                        village['Телефон основной'] = phone
            
            # Поиск телефона в тексте карточки
            if not village.get('Телефон основной'):
                phone = self.extract_phone(card_text)
                if phone:
                    village['Телефон основной'] = phone
            
            # Если телефон не найден и есть ссылка на детальную страницу, используем Selenium
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    logger.debug(f"Попытка получить телефон через Selenium для {village.get('Название поселка')}")
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                        logger.info(f"Телефон получен через Selenium: {phone}")
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            # Поиск email
            email_elem = card.find('a', href=re.compile(r'mailto:', re.I))
            if email_elem:
                email = email_elem.get('href', '').replace('mailto:', '').strip()
                # Исключаем служебные email
                if 'cian.ru' not in email.lower() and 'support' not in email.lower():
                    village['Email'] = email
            else:
                email = self.extract_email(card_text)
                if email and 'cian.ru' not in email.lower() and 'support' not in email.lower():
                    village['Email'] = email
            
            # Валидация данных перед возвратом
            if not village['Название поселка'] or not self.validate_village_name(village['Название поселка']):
                return None
            
            # Проверка наличия ссылки на поселок
            if village['Ссылка на источник'] and not self.is_village_url(village['Ссылка на источник']):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге карточки Cian.ru: {e}")
        
        return None
    
    def parse_domclick(self, region: str = 'moskovskaya-oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг DomClick.ru через Selenium (требует авторизацию)"""
        logger.info(f"Начинаем парсинг DomClick.ru для региона: {region}")
        villages = []
        
        if not self.use_selenium or not self.selenium_extractor:
            logger.warning("Для парсинга DomClick.ru требуется Selenium")
            return villages
        
        try:
            driver = self.selenium_extractor.driver
            # DomClick использует поиск по участкам
            base_url = "https://domclick.ru/search"
            
            for page in range(1, max_pages + 1):
                self.delay()
                
                # Формируем URL для поиска участков в МО
                if page == 1:
                    url = f"{base_url}?region=moscow_region&category=land&object_type=plot"
                else:
                    url = f"{base_url}?region=moscow_region&category=land&object_type=plot&page={page}"
                
                try:
                    logger.debug(f"Загрузка страницы {page}: {url}")
                    driver.get(url)
                    
                    # Ждем загрузки
                    time.sleep(5)
                    
                    # Прокручиваем страницу
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Проверяем на авторизацию
                    if 'войти' in page_source.lower() or 'авторизация' in page_source.lower():
                        logger.warning(f"Требуется авторизация на DomClick.ru для страницы {page}")
                        break
                    
                    # Ищем карточки объявлений
                    cards = soup.find_all(['div', 'article'], class_=re.compile(r'card|item|offer|snippet', re.I))
                    
                    # Фильтруем карточки с упоминаниями поселков
                    filtered_cards = []
                    for card in cards:
                        card_text = card.get_text().lower()
                        if any(keyword in card_text for keyword in ['поселок', 'коттеджный', 'кп', 'коттеджный поселок']):
                            filtered_cards.append(card)
                    
                    if not filtered_cards:
                        filtered_cards = cards[:30]  # Берем первые для проверки
                    
                    logger.debug(f"Найдено карточек на странице {page}: {len(filtered_cards)}")
                    
                    for card in filtered_cards:
                        village_data = self._parse_domclick_card(card, url)
                        if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                            villages.append(village_data)
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено поселков: {len([v for v in villages if v])}")
                    
                    if not cards:
                        logger.warning(f"Не найдено карточек на странице {page}")
                        break
                        
                except TimeoutException:
                    logger.warning(f"Таймаут при загрузке страницы {page} DomClick.ru")
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} DomClick.ru: {e}")
                    continue
            
            logger.info(f"Всего найдено поселков на DomClick.ru: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге DomClick.ru: {e}")
        
        return villages
    
    def _parse_domclick_card(self, card, source_url: str) -> Optional[Dict]:
        """Парсинг карточки поселка с DomClick.ru"""
        try:
            village = {
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
                'Источник информации': 'DomClick.ru',
                'Дата добавления в базу': self.current_date,
                'Дата последнего обновления': self.current_date,
                'Кто добавил': 'Парсер'
            }
            
            card_text = card.get_text()
            
            # Поиск ссылки
            link = card.find('a', href=True)
            if link:
                href = link.get('href', '')
                if href.startswith('/'):
                    href = urljoin('https://domclick.ru', href)
                elif not href.startswith('http'):
                    href = urljoin('https://domclick.ru', '/' + href)
                village['Ссылка на источник'] = href
            
            # Поиск названия
            title_elem = card.find(['h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|name|heading', re.I))
            if not title_elem:
                title_elem = card.find(['h2', 'h3', 'h4'])
            
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if self.validate_village_name(title_text):
                    village['Название поселка'] = title_text
            
            # Если не нашли, ищем в тексте
            if not village.get('Название поселка'):
                text_lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                for line in text_lines[:10]:
                    if 'поселок' in line.lower() or 'коттеджный' in line.lower():
                        line_clean = re.sub(r'[^\w\s\-]', '', line).strip()
                        if self.validate_village_name(line_clean) and len(line_clean) > 5:
                            village['Название поселка'] = line_clean
                            break
            
            # Поиск адреса
            address_elem = card.find(['span', 'div'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                village['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск телефона
            phone = self.extract_phone(card_text)
            if phone:
                village['Телефон основной'] = phone
            
            # Если телефон не найден и есть ссылка, используем Selenium
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            if not village['Название поселка'] or not self.validate_village_name(village['Название поселка']):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.debug(f"Ошибка при парсинге карточки DomClick.ru: {e}")
            return None
    
    def parse_yandex_realty(self, region: str = 'moskovskaya-oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг Яндекс.Недвижимость через Selenium (динамический контент)"""
        logger.info(f"Начинаем парсинг Яндекс.Недвижимость для региона: {region}")
        villages = []
        
        if not self.use_selenium or not self.selenium_extractor:
            logger.warning("Для парсинга Яндекс.Недвижимость требуется Selenium")
            return villages
        
        try:
            # Яндекс.Недвижимость использует динамический контент, нужен Selenium
            # Используем страницу с коттеджными поселками, а не отдельными участками
            base_url = "https://realty.yandex.ru/moskva_i_mo/kupit/kottedzhnye-poselki/"
            
            driver = self.selenium_extractor.driver
            
            for page in range(1, max_pages + 1):
                self.delay()
                
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?page={page}"
                
                try:
                    logger.debug(f"Загрузка страницы {page}: {url}")
                    driver.get(url)
                    
                    # Ждем загрузки контента (увеличиваем таймаут)
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "article, .offer, .item, [data-test], .OffersSerp, .OffersSerpItem"))
                        )
                    except TimeoutException:
                        # Пробуем найти любые элементы на странице
                        logger.debug("Ожидание загрузки контента...")
                        self.delay()
                        time.sleep(2)
                    
                    # Прокручиваем страницу для загрузки динамического контента
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    
                    # Получаем HTML после загрузки
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Ищем карточки объявлений (разные варианты селекторов для Яндекс.Недвижимость)
                    cards = soup.find_all(['article', 'div'], class_=re.compile(r'OffersSerpItem|offer|item|card|snippet|SerpItem', re.I))
                    
                    # Фильтруем карточки, которые содержат упоминания поселков
                    filtered_cards = []
                    for card in cards:
                        card_text = card.get_text().lower()
                        # Ищем упоминания поселков в тексте карточки
                        if any(keyword in card_text for keyword in ['поселок', 'коттеджный', 'кп', 'коттеджный поселок', 'коттеджный посёлок']):
                            filtered_cards.append(card)
                    
                    # Если не нашли через фильтр, используем все карточки
                    if not filtered_cards and cards:
                        filtered_cards = cards[:50]  # Ограничиваем количество для проверки
                    
                    # Также ищем по data-атрибутам
                    if not filtered_cards:
                        cards_by_data = soup.find_all(attrs={'data-test': re.compile(r'offer|item|serp-item', re.I)})
                        for card in cards_by_data:
                            card_text = card.get_text().lower()
                            if any(keyword in card_text for keyword in ['поселок', 'коттеджный', 'кп']):
                                filtered_cards.append(card)
                    
                    # Если все еще не нашли, ищем ссылки на поселки
                    if not filtered_cards:
                        links = soup.find_all('a', href=re.compile(r'/offer|/uchastok|/kottedzhnye', re.I))
                        for link in links:
                            link_text = link.get_text().lower()
                            if any(keyword in link_text for keyword in ['поселок', 'коттеджный', 'кп']):
                                parent = link.find_parent(['div', 'article'])
                                if parent and parent not in filtered_cards:
                                    filtered_cards.append(parent)
                    
                    logger.info(f"Найдено карточек на странице {page}: {len(filtered_cards)} (из {len(cards)} всего)")
                    
                    parsed_count = 0
                    for card in filtered_cards:
                        village_data = self._parse_yandex_card(card, url)
                        if village_data:
                            if self.validate_village_name(village_data.get('Название поселка', '')):
                                villages.append(village_data)
                                parsed_count += 1
                            else:
                                logger.debug(f"Карточка не прошла валидацию: название='{village_data.get('Название поселка', 'N/A')}'")
                    
                    logger.debug(f"Обработано карточек: {parsed_count} валидных из {len(filtered_cards)}")
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено поселков: {len([v for v in villages if v])}")
                    
                    # Если не нашли карточек, возможно, это последняя страница
                    if not cards:
                        logger.warning(f"Не найдено карточек на странице {page}")
                        break
                        
                except TimeoutException:
                    logger.warning(f"Таймаут при загрузке страницы {page} Яндекс.Недвижимость")
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} Яндекс.Недвижимость: {e}")
                    continue
            
            logger.info(f"Всего найдено поселков на Яндекс.Недвижимость: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Яндекс.Недвижимость: {e}")
        
        return villages
    
    def _parse_yandex_card(self, card, source_url: str) -> Optional[Dict]:
        """Парсинг карточки поселка с Яндекс.Недвижимость"""
        try:
            village = {
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
                'Источник информации': 'Яндекс.Недвижимость',
                'Дата добавления в базу': self.current_date,
                'Дата последнего обновления': self.current_date,
                'Кто добавил': 'Парсер'
            }
            
            card_text = card.get_text()
            
            # Поиск ссылки
            link = card.find('a', href=True)
            if link:
                href = link.get('href', '')
                if href.startswith('/'):
                    href = urljoin('https://realty.yandex.ru', href)
                elif not href.startswith('http'):
                    href = urljoin('https://realty.yandex.ru', '/' + href)
                village['Ссылка на источник'] = href
            
            # Поиск названия
            title_elem = card.find(['h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|name|heading|link|OfferTitle', re.I))
            if not title_elem:
                title_elem = card.find(['h2', 'h3', 'h4'])
            
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if self.validate_village_name(title_text):
                    village['Название поселка'] = title_text
            
            # Если не нашли, ищем в тексте карточки (приоритет строкам с "поселок")
            if not village.get('Название поселка'):
                text_lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                # Сначала ищем строки с упоминанием поселка
                for line in text_lines:
                    if 'поселок' in line.lower() or 'коттеджный' in line.lower():
                        # Извлекаем название поселка из строки
                        # Пытаемся найти название до или после слова "поселок"
                        parts = re.split(r'поселок|коттеджный', line, flags=re.I)
                        for part in parts:
                            part_clean = re.sub(r'[^\w\s\-]', '', part).strip()
                            if self.validate_village_name(part_clean) and len(part_clean) > 5:
                                village['Название поселка'] = part_clean
                                break
                        if village.get('Название поселка'):
                            break
                
                # Если не нашли, ищем любую валидную строку
                if not village.get('Название поселка'):
                    for line in text_lines[:10]:
                        line_clean = re.sub(r'[^\w\s\-]', '', line).strip()
                        if self.validate_village_name(line_clean) and len(line_clean) > 5:
                            village['Название поселка'] = line_clean
                            break
            
            # Поиск адреса
            address_elem = card.find(['span', 'div'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                village['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск телефона
            phone = self.extract_phone(card_text)
            if phone:
                village['Телефон основной'] = phone
            
            # Если телефон не найден и есть ссылка, используем Selenium
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            if not village['Название поселка'] or not self.validate_village_name(village['Название поселка']):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.debug(f"Ошибка при парсинге карточки Яндекс.Недвижимость: {e}")
            return None
    
    def parse_cottage_ru(self, max_pages: int = 5) -> List[Dict]:
        """Парсинг Cottage.ru"""
        logger.info("Начинаем парсинг Cottage.ru")
        villages = []
        
        try:
            base_url = "https://cottage.ru/poselki"
            
            # Cottage.ru блокирует requests, используем Selenium
            use_selenium = self.use_selenium and self.selenium_extractor
            if use_selenium:
                driver = self.selenium_extractor.driver
            
            for page in range(1, max_pages + 1):
                self.delay()
                
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?page={page}"
                
                try:
                    if use_selenium:
                        logger.debug(f"Загрузка страницы {page} Cottage.ru через Selenium")
                        driver.get(url)
                        time.sleep(5)
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)
                        page_source = driver.page_source
                        soup = BeautifulSoup(page_source, 'html.parser')
                    else:
                        response = self.session.get(url, timeout=60)
                        
                        # Проверяем на блокировку
                        if response.status_code == 429:
                            logger.warning(f"Получен код 429 для страницы {page} Cottage.ru. Рекомендуется использовать Selenium.")
                            time.sleep(15)
                            response = self.session.get(url, timeout=60)
                        
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Ищем карточки поселков
                    cards = soup.find_all(['div', 'article'], class_=re.compile(r'village|poselok|item|card|descr', re.I))
                    
                    # Также ищем ссылки на поселки
                    links = soup.find_all('a', href=re.compile(r'/poselok|/village|/poselki', re.I))
                    
                    # Обрабатываем ссылки
                    processed_links = set()
                    for link in links:
                        href = link.get('href', '')
                        if href in processed_links:
                            continue
                        processed_links.add(href)
                        
                        parent = link.find_parent(['div', 'article', 'section'])
                        if not parent:
                            parent = link
                        
                        village_data = self._parse_cottage_card(parent, url, link_href=href)
                        if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                            villages.append(village_data)
                    
                    # Обрабатываем карточки
                    for card in cards:
                        village_data = self._parse_cottage_card(card, url)
                        if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                            villages.append(village_data)
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено поселков: {len([v for v in villages if v])}")
                    
                    if not cards and not links:
                        logger.warning(f"Не найдено карточек на странице {page}")
                        break
                        
                except requests.exceptions.Timeout:
                    logger.warning(f"Таймаут при загрузке страницы {page} Cottage.ru")
                    continue
                except requests.exceptions.RequestException as e:
                    logger.error(f"Ошибка сети при парсинге страницы {page} Cottage.ru: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} Cottage.ru: {e}")
                    continue
            
            logger.info(f"Всего найдено поселков на Cottage.ru: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Cottage.ru: {e}")
        
        return villages
    
    def _parse_cottage_card(self, card, source_url: str, link_href: str = None) -> Optional[Dict]:
        """Парсинг карточки поселка с Cottage.ru"""
        try:
            village = {
                'ID': self.village_id,
                'Название поселка': None,
                'Регион': None,
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
            
            card_text = card.get_text()
            
            # Поиск ссылки
            if link_href:
                if link_href.startswith('/'):
                    village['Ссылка на источник'] = urljoin('https://cottage.ru', link_href)
                else:
                    village['Ссылка на источник'] = link_href
            else:
                link = card.find('a', href=re.compile(r'/poselok|/village|/poselki', re.I))
                if link:
                    href = link.get('href', '')
                    if href.startswith('/'):
                        href = urljoin('https://cottage.ru', href)
                    village['Ссылка на источник'] = href
            
            # Поиск названия
            title_elem = card.find(['h1', 'h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|name|heading', re.I))
            if not title_elem:
                title_elem = card.find(['h1', 'h2', 'h3', 'h4'])
            
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if self.validate_village_name(title_text):
                    village['Название поселка'] = title_text
            
            # Если не нашли, ищем в тексте
            if not village.get('Название поселка'):
                text_lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                for line in text_lines[:10]:
                    if 'поселок' in line.lower() or 'коттеджный' in line.lower():
                        line_clean = re.sub(r'[^\w\s\-]', '', line).strip()
                        if self.validate_village_name(line_clean) and len(line_clean) > 5:
                            village['Название поселка'] = line_clean
                            break
            
            # Поиск адреса
            address_elem = card.find(['span', 'div'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                village['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск телефона
            phone = self.extract_phone(card_text)
            if phone:
                village['Телефон основной'] = phone
            
            # Если телефон не найден и есть ссылка, используем Selenium
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            if not village['Название поселка'] or not self.validate_village_name(village['Название поселка']):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.debug(f"Ошибка при парсинге карточки Cottage.ru: {e}")
            return None
    
    def parse_poselki_ru(self, max_pages: int = 5) -> List[Dict]:
        """Парсинг Poselki.ru"""
        logger.info("Начинаем парсинг Poselki.ru")
        villages = []
        
        try:
            base_url = "https://poselki.ru"
            
            for page in range(1, max_pages + 1):
                self.delay()
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?page={page}"
                
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Поиск ссылок на поселки (основной метод для Poselki.ru)
                    poselok_links = soup.find_all('a', href=True)
                    poselok_links = [link for link in poselok_links 
                                    if ('poselok' in link.get('href', '').lower() or 
                                        'поселок' in link.get_text().lower() or
                                        'village' in link.get('href', '').lower()) and
                                    link.get('href', '').startswith('http')]
                    
                    # Также ищем карточки
                    cards = soup.find_all(['div', 'article'], class_=re.compile(r'village|poselok|item|card|descr', re.I))
                    
                    # Обрабатываем ссылки на поселки
                    processed_links = set()
                    for link in poselok_links:
                        href = link.get('href', '')
                        if href in processed_links:
                            continue
                        processed_links.add(href)
                        
                        # Находим родительский элемент для получения полной информации
                        parent = link.find_parent(['div', 'article', 'section'])
                        if not parent:
                            parent = link
                        
                        village_data = self._parse_poselki_card(parent, url, link_href=href)
                        if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                            villages.append(village_data)
                    
                    # Если не нашли через ссылки, обрабатываем карточки
                    if not villages and cards:
                        for card in cards:
                            village_data = self._parse_poselki_card(card, url)
                            if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                                villages.append(village_data)
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено поселков: {len([v for v in villages if v])}")
                    
                    if not cards:
                        logger.warning(f"Не найдено карточек на странице {page}")
                        break
                        
                except requests.exceptions.Timeout:
                    logger.warning(f"Таймаут при загрузке страницы {page} Poselki.ru")
                    continue
                except requests.exceptions.RequestException as e:
                    logger.error(f"Ошибка сети при парсинге страницы {page} Poselki.ru: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} Poselki.ru: {e}")
                    continue
            
            logger.info(f"Всего найдено поселков на Poselki.ru: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Poselki.ru: {e}")
        
        return villages
    
    def _parse_poselki_card(self, card, source_url: str, link_href: str = None) -> Optional[Dict]:
        """Парсинг карточки поселка с Poselki.ru"""
        try:
            village = {
                'ID': self.village_id,
                'Название поселка': None,
                'Регион': None,
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
                'Источник информации': 'Poselki.ru',
                'Дата добавления в базу': self.current_date,
                'Дата последнего обновления': self.current_date,
                'Кто добавил': 'Парсер'
            }
            
            card_text = card.get_text()
            
            # Поиск ссылки
            if link_href:
                village['Ссылка на источник'] = link_href
            else:
                link = card.find('a', href=re.compile(r'poselok|village|поселок', re.I))
                if link:
                    href = link.get('href', '')
                    if href.startswith('/'):
                        href = urljoin('https://poselki.ru', href)
                    elif not href.startswith('http'):
                        href = urljoin('https://poselki.ru', '/' + href)
                    village['Ссылка на источник'] = href
            
            # Поиск названия
            # Сначала ищем в ссылке, если она передана
            if link_href:
                # Ищем ссылку с этим href в карточке
                link_elem = card.find('a', href=link_href)
                if not link_elem:
                    # Если не нашли, ищем любую ссылку в карточке
                    link_elem = card.find('a', href=True)
                
                if link_elem:
                    title_text = link_elem.get_text(strip=True)
                    # Очищаем текст от лишних символов
                    title_text = re.sub(r'\s+', ' ', title_text).strip()
                    # Извлекаем название из текста (убираем лишние слова)
                    if 'поселок' in title_text.lower():
                        # Пытаемся извлечь название до или после слова "поселок"
                        parts = re.split(r'поселок', title_text, flags=re.I)
                        for part in parts:
                            part = part.strip()
                            if len(part) > 5 and self.validate_village_name(part):
                                title_text = part
                                break
                    
                    if self.validate_village_name(title_text):
                        village['Название поселка'] = title_text
            
            # Если не нашли, ищем в заголовках
            if not village.get('Название поселка'):
                title_elem = card.find(['h1', 'h2', 'h3', 'h4'], class_=re.compile(r'title|name|heading', re.I))
                if not title_elem:
                    title_elem = card.find(['h1', 'h2', 'h3', 'h4'])
                
                if title_elem:
                    title_text = title_elem.get_text(strip=True)
                    if self.validate_village_name(title_text):
                        village['Название поселка'] = title_text
            
            # Если все еще не нашли, ищем в тексте карточки
            if not village.get('Название поселка'):
                # Ищем текст, который похож на название поселка
                text_lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                for line in text_lines[:10]:  # Проверяем первые 10 строк
                    # Очищаем от лишних символов
                    line = re.sub(r'[^\w\s\-]', '', line).strip()
                    if self.validate_village_name(line) and len(line) > 5:
                        village['Название поселка'] = line
                        break
            
            # Поиск адреса
            address_elem = card.find(['span', 'div'], class_=re.compile(r'address|location', re.I))
            if address_elem:
                village['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск телефона
            phone = self.extract_phone(card_text)
            if phone:
                village['Телефон основной'] = phone
            
            # Если телефон не найден и есть ссылка, используем Selenium
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            if not village['Название поселка'] or not self.validate_village_name(village['Название поселка']):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге карточки Poselki.ru: {e}")
            return None
    
    def parse_avito(self, region: str = 'moskovskaya_oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг Авито Недвижимость через Selenium (из-за блокировок)"""
        logger.info(f"Начинаем парсинг Авито для региона: {region}")
        villages = []
        
        # Avito блокирует обычные запросы, используем Selenium
        if not self.use_selenium or not self.selenium_extractor:
            logger.warning("Для парсинга Авито рекомендуется использовать Selenium из-за блокировок")
            # Пробуем без Selenium, но с большими задержками
            use_selenium = False
        else:
            use_selenium = True
            driver = self.selenium_extractor.driver
        
        try:
            # Используем страницу с земельными участками, там есть коттеджные поселки
            base_url = f"https://www.avito.ru/{region}/zemelnye_uchastki"
            
            for page in range(1, max_pages + 1):
                self.delay()
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?p={page}"
                
                try:
                    if use_selenium:
                        # Используем Selenium для обхода блокировок
                        logger.debug(f"Загрузка страницы {page} через Selenium: {url}")
                        driver.get(url)
                        
                        # Ждем загрузки
                        time.sleep(5)
                        
                        # Прокручиваем страницу
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)
                        
                        page_source = driver.page_source
                        soup = BeautifulSoup(page_source, 'html.parser')
                        
                        # Проверяем на капчу
                        if 'captcha' in page_source.lower() or 'доступ ограничен' in page_source.lower():
                            logger.warning(f"Обнаружена капча или блокировка на странице {page} Авито")
                            break
                    else:
                        # Пробуем через обычные запросы с большими задержками
                        time.sleep(5)  # Увеличиваем задержку
                        
                        response = self.session.get(url, timeout=60)
                        
                        # Проверяем на блокировку
                        if response.status_code == 429:
                            logger.warning(f"Получен код 429 (Too Many Requests) для страницы {page}. Пропускаем.")
                            time.sleep(15)
                            continue
                        
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Проверяем на страницу с капчей или блокировкой
                        if 'captcha' in response.text.lower() or 'доступ ограничен' in response.text.lower():
                            logger.warning(f"Обнаружена капча или блокировка на странице {page} Авито")
                            break
                    
                    # Поиск карточек объявлений на Авито
                    # Avito использует data-marker="catalog-serp-item" или "item" для карточек
                    cards = soup.find_all('div', attrs={'data-marker': re.compile(r'catalog-serp-item|item|card|iva-item', re.I)})
                    
                    # Если не нашли по data-marker, ищем по классам
                    if not cards:
                        cards = soup.find_all(['div', 'article'], class_=re.compile(r'iva-item|item|card|snippet|serp-item', re.I))
                    
                    # Также ищем по структуре - карточки обычно в контейнерах
                    if not cards:
                        # Ищем контейнеры с объявлениями
                        containers = soup.find_all(['div', 'section'], class_=re.compile(r'items|list|catalog|serp', re.I))
                        for container in containers:
                            container_cards = container.find_all('div', attrs={'data-marker': True})
                            cards.extend(container_cards)
                    
                    # Фильтруем карточки, которые содержат упоминания поселков
                    filtered_cards = []
                    for card in cards:
                        card_text = card.get_text().lower()
                        # Ищем упоминания поселков в тексте карточки
                        if any(keyword in card_text for keyword in ['поселок', 'коттеджный', 'кп', 'коттеджный поселок', 'коттеджный посёлок']):
                            filtered_cards.append(card)
                    
                    # Если не нашли через фильтр, но есть карточки, проверяем их по отдельности
                    if not filtered_cards and cards:
                        # Берем первые карточки для проверки
                        filtered_cards = cards[:30]  # Ограничиваем для проверки
                    
                    cards = filtered_cards
                    logger.debug(f"Найдено карточек на странице {page}: {len(cards)} (из {len(soup.find_all('div', attrs={'data-marker': True}))} всего с data-marker)")
                    
                    for card in cards:
                        village_data = self._parse_avito_card(card, url)
                        if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                            villages.append(village_data)
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено поселков: {len([v for v in villages if v])}")
                    
                    if not cards:
                        logger.warning(f"Не найдено карточек на странице {page}")
                        break
                        
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} Авито: {e}")
                    continue
            
            logger.info(f"Всего найдено поселков на Авито: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Авито: {e}")
        
        return villages
    
    def _parse_avito_card(self, card, source_url: str) -> Optional[Dict]:
        """Парсинг карточки поселка с Авито"""
        try:
            village = {
                'ID': self.village_id,
                'Название поселка': None,
                'Регион': None,
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
                'Источник информации': 'Авито',
                'Дата добавления в базу': self.current_date,
                'Дата последнего обновления': self.current_date,
                'Кто добавил': 'Парсер'
            }
            
            card_text = card.get_text()
            
            # Поиск ссылки (Avito использует data-marker для ссылок)
            link = card.find('a', href=re.compile(r'/moskovskaya_oblast/zemelnye_uchastki|/kottedzhnye_poselki|/moskva/zemelnye_uchastki', re.I))
            if not link:
                # Ищем любую ссылку в карточке
                link = card.find('a', href=True)
            
            if link:
                href = link.get('href', '')
                if href.startswith('/'):
                    href = urljoin('https://www.avito.ru', href)
                elif not href.startswith('http'):
                    href = urljoin('https://www.avito.ru', '/' + href)
                village['Ссылка на источник'] = href
            
            # Поиск названия
            # Avito использует h3 с классом title-root или data-marker="item-title"
            title_elem = card.find('h3', attrs={'data-marker': re.compile(r'item-title|title', re.I)})
            if not title_elem:
                title_elem = card.find(['h3', 'h2'], class_=re.compile(r'title|name|heading', re.I))
            if not title_elem:
                title_elem = card.find('a', href=re.compile(r'zemelnye_uchastki|kottedzhnye', re.I))
            
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                # Очищаем от лишних символов
                title_text = re.sub(r'\s+', ' ', title_text).strip()
                if self.validate_village_name(title_text):
                    village['Название поселка'] = title_text
            
            # Если не нашли, ищем в тексте карточки
            if not village.get('Название поселка'):
                text_lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                for line in text_lines[:10]:
                    if 'поселок' in line.lower() or 'коттеджный' in line.lower():
                        line_clean = re.sub(r'[^\w\s\-]', '', line).strip()
                        if self.validate_village_name(line_clean) and len(line_clean) > 5:
                            village['Название поселка'] = line_clean
                            break
            
            # Поиск адреса
            address_elem = card.find(['span', 'div'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                village['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск телефона
            phone_elem = card.find(['a', 'span'], href=re.compile(r'tel:', re.I))
            if phone_elem:
                phone = phone_elem.get('href', '').replace('tel:', '').strip()
                village['Телефон основной'] = phone
            else:
                phone = self.extract_phone(card_text)
                if phone:
                    village['Телефон основной'] = phone
            
            # Если телефон не найден и есть ссылка, используем Selenium
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            if not village['Название поселка'] or not self.validate_village_name(village['Название поселка']):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге карточки Авито: {e}")
            return None
    
    def enrich_village_data(self, village: Dict) -> Dict:
        """Дополнение данных о поселке из детальной страницы"""
        if not village.get('Ссылка на источник'):
            return village
        
        try:
            self.delay()
            response = self.session.get(village['Ссылка на источник'], timeout=(10, 25))  # (connect, read)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Поиск дополнительной информации на странице
            page_text = soup.get_text()
            
            # Улучшенный поиск количества домов/участков
            if not village.get('Количество домов/участков'):
                houses_patterns = [
                    r'(\d+)\s*(?:дом|участк|коттедж|лот)\s*в\s*продаже',
                    r'в\s*продаже\s*(\d+)\s*(?:дом|участк|коттедж|лот)',
                    r'(\d+)\s*(?:дом|участк|коттедж|лот)',
                    r'количество[:\s]+(\d+)',
                ]
                
                for pattern in houses_patterns:
                    match = re.search(pattern, page_text, re.I)
                    if match:
                        try:
                            houses_num = int(match.group(1))
                            if 10 <= houses_num <= 10000:  # Разумные пределы
                                village['Количество домов/участков'] = houses_num
                                break
                        except (ValueError, IndexError):
                            continue
            
            # Улучшенный поиск информации об ограждении
            if not village.get('Наличие ограждения'):
                if re.search(r'огражден|забор|периметр|ограждение|защищен', page_text, re.I):
                    village['Наличие ограждения'] = 'Да'
            
            # Улучшенный поиск информации о КПП
            if not village.get('Наличие КПП'):
                if re.search(r'кпп|контрольно-пропускной|пропускной\s*пункт|контроль\s*доступа', page_text, re.I):
                    village['Наличие КПП'] = 'Да'
                    
                    # Поиск количества КПП
                    kpp_patterns = [
                        r'(\d+)\s*кпп',
                        r'кпп[:\s]+(\d+)',
                    ]
                    for pattern in kpp_patterns:
                        match = re.search(pattern, page_text, re.I)
                        if match:
                            try:
                                village['Количество КПП'] = int(match.group(1))
                                break
                            except (ValueError, IndexError):
                                continue
            
            # Улучшенный поиск информации об охране
            if not village.get('Наличие охраны'):
                if re.search(r'охрана|чоп|охраняем|безопасность|сторож', page_text, re.I):
                    village['Наличие охраны'] = 'Да'
                    
                    # Определение типа охраны
                    if re.search(r'чоп|частн.*охранн', page_text, re.I):
                        village['Тип охраны'] = 'ЧОП'
                    elif re.search(r'собственн.*охрана|внутренн.*охрана', page_text, re.I):
                        village['Тип охраны'] = 'Собственная охрана'
            
            # Поиск информации о статусе поселка
            if not village.get('Статус поселка'):
                if re.search(r'построен|заселен|сдан|эксплуат', page_text, re.I):
                    village['Статус поселка'] = 'Построен и заселен'
                elif re.search(r'сдача\s*в\s*\d{4}|строительств|возводится', page_text, re.I):
                    village['Статус поселка'] = 'В строительстве (>80%)'
            
            # Поиск информации об интернете
            if re.search(r'интернет|wi-fi|wifi|подключен.*интернет', page_text, re.I):
                village['Наличие интернета'] = 'Да'
            
            # Улучшенный поиск телефона
            if not village.get('Телефон основной'):
                # Поиск в специальных блоках
                phone_blocks = soup.find_all(['div', 'span'], class_=re.compile(r'phone|contact|tel', re.I))
                for block in phone_blocks:
                    phone = self.extract_phone(block.get_text())
                    if phone:
                        village['Телефон основной'] = phone
                        break
                
                # Если не нашли в блоках, ищем в тексте
                if not village.get('Телефон основной'):
                    phone = self.extract_phone(page_text)
                    if phone:
                        village['Телефон основной'] = phone
                
                # Если телефон все еще не найден, используем Selenium
                if not village.get('Телефон основной') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                    try:
                        logger.debug(f"Попытка получить телефон через Selenium для {village.get('Название поселка')}")
                        self.delay()
                        phone = self.selenium_extractor.extract_phone_from_url(village['Ссылка на источник'])
                        if phone:
                            village['Телефон основной'] = phone
                            self.selenium_phones_count += 1
                            logger.info(f"Телефон получен через Selenium: {phone}")
                    except Exception as e:
                        logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            # Улучшенный поиск email
            if not village.get('Email'):
                email_blocks = soup.find_all(['a', 'span'], class_=re.compile(r'email|mail|contact', re.I))
                for block in email_blocks:
                    email = self.extract_email(block.get_text())
                    if email and 'cian.ru' not in email.lower() and 'support' not in email.lower():
                        village['Email'] = email
                        break
                
                # Если не нашли в блоках, ищем в тексте
                if not village.get('Email'):
                    email = self.extract_email(page_text)
                    if email and 'cian.ru' not in email.lower() and 'support' not in email.lower():
                        village['Email'] = email
            
            # Поиск сайта поселка/УК
            if not village.get('Сайт поселка/УК'):
                # Ищем ссылки на внешние сайты
                external_links = soup.find_all('a', href=re.compile(r'^https?://', re.I))
                for link in external_links:
                    href = link.get('href', '')
                    # Исключаем ссылки на соцсети и агрегаторы
                    excluded_domains = ['cian.ru', 'domclick.ru', 'yandex.ru', 'google.com', 
                                       'vk.com', 'facebook.com', 'instagram.com', 'ok.ru']
                    if not any(domain in href.lower() for domain in excluded_domains):
                        village['Сайт поселка/УК'] = href
                        break
            
            # Поиск названия УК/ТСЖ
            if not village.get('Название УК/ТСЖ'):
                uk_patterns = [
                    r'управляющ.*компани[яи]?[:\s]+([А-ЯЁ][А-Яа-яё\s«»""]+)',
                    r'ук[:\s]+([А-ЯЁ][А-Яа-яё\s«»""]+)',
                    r'тсж[:\s]+([А-ЯЁ][А-Яа-яё\s«»""]+)',
                ]
                for pattern in uk_patterns:
                    match = re.search(pattern, page_text, re.I)
                    if match:
                        uk_name = match.group(1).strip()
                        if len(uk_name) > 3:
                            village['Название УК/ТСЖ'] = uk_name
                            break
            
            # Обновляем дату последнего обновления
            village['Дата последнего обновления'] = self.current_date
            
        except Exception as e:
            logger.warning(f"Не удалось обогатить данные для {village.get('Название поселка')}: {e}")
        
        return village
    
    def assess_target_client(self, village: Dict) -> str:
        """Оценка, является ли поселок целевым клиентом"""
        score = 0
        
        # Проверка обязательных критериев
        if village.get('Количество домов/участков'):
            try:
                houses_num = village['Количество домов/участков']
                if isinstance(houses_num, str):
                    houses_num = int(houses_num) if houses_num.isdigit() else 0
                else:
                    houses_num = int(houses_num) if houses_num else 0
                if houses_num >= 20:
                    score += 1
            except (ValueError, TypeError):
                pass
        
        if village.get('Наличие ограждения') == 'Да':
            score += 1
        
        if village.get('Наличие КПП') == 'Да':
            score += 1
        
        if village.get('Наличие охраны') == 'Да':
            score += 1
        
        if village.get('Статус поселка'):
            if 'построен' in village['Статус поселка'].lower():
                score += 1
        
        # Оценка
        if score >= 4:
            return 'Целевой'
        elif score >= 2:
            return 'Требует проверки'
        else:
            return 'Нецелевой'
    
    def load_existing_results(self, filename: str = 'Результат парсинга информации в интернете.csv') -> List[Dict]:
        """Загрузка существующих результатов из CSV файла"""
        existing_results = []
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    existing_results = list(reader)
                logger.info(f"Загружено существующих записей: {len(existing_results)}")
            except Exception as e:
                logger.warning(f"Не удалось загрузить существующие данные: {e}")
        return existing_results
    
    def save_results(self, filename: str = 'Результат парсинга информации в интернете.csv', append: bool = True):
        """Сохранение результатов в CSV файл
        
        Args:
            filename: Имя файла для сохранения
            append: Если True, дополняет существующий файл, иначе перезаписывает
        """
        if not self.results:
            logger.warning("Нет данных для сохранения")
            return
        
        try:
            # Загружаем существующие данные, если нужно дополнять
            existing_results = []
            if append:
                existing_results = self.load_existing_results(filename)
            
            # Объединяем данные: сначала существующие, потом новые
            all_results = existing_results + self.results
            
            # Удаляем дубликаты по названию и адресу
            unique_results = {}
            max_id = 0
            
            # Сначала находим максимальный ID из существующих записей
            for village in existing_results:
                village_id = village.get('ID')
                if village_id:
                    try:
                        if isinstance(village_id, str):
                            village_id = int(village_id) if village_id.isdigit() else 0
                        else:
                            village_id = int(village_id)
                        max_id = max(max_id, village_id)
                    except:
                        pass
            
            for village in all_results:
                # Оценка целевого клиента перед сохранением
                village['Оценка целевого клиента'] = self.assess_target_client(village)
                
                # Получаем ID или присваиваем новый
                village_id = village.get('ID')
                if village_id:
                    try:
                        if isinstance(village_id, str):
                            village_id = int(village_id) if village_id.isdigit() else None
                        else:
                            village_id = int(village_id)
                        if village_id:
                            max_id = max(max_id, village_id)
                    except:
                        village_id = None
                
                if not village_id:
                    max_id += 1
                    village_id = max_id
                    village['ID'] = village_id
                
                # Создаем ключ для дедупликации
                name = village.get('Название поселка', '').lower().strip()
                address = village.get('Адрес', '').lower().strip() if village.get('Адрес') else ''
                name_normalized = re.sub(r'[^\w\s]', '', name)
                name_normalized = re.sub(r'\s+', ' ', name_normalized).strip()
                
                key = (name_normalized, address) if address else (name_normalized,)
                
                # Если запись с таким ключом уже есть, выбираем более полную
                if key in unique_results:
                    existing = unique_results[key]
                    # Подсчитываем полноту данных
                    existing_score = sum(1 for f in ['Адрес', 'Телефон основной', 'Email', 'Количество домов/участков', 'Ссылка на источник'] 
                                       if existing.get(f))
                    village_score = sum(1 for f in ['Адрес', 'Телефон основной', 'Email', 'Количество домов/участков', 'Ссылка на источник'] 
                                      if village.get(f))
                    
                    # Если новая запись более полная, заменяем
                    if village_score > existing_score:
                        unique_results[key] = village
                    else:
                        # Иначе объединяем данные (заполняем пустые поля)
                        for field in FIELDS:
                            if not existing.get(field) and village.get(field):
                                existing[field] = village[field]
                else:
                    unique_results[key] = village
            
            # Сохраняем все уникальные результаты
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
                writer.writeheader()
                
                # Сортируем по ID для сохранения порядка
                def get_id(v):
                    try:
                        id_val = v.get('ID', 0)
                        if isinstance(id_val, str):
                            return int(id_val) if id_val.isdigit() else 0
                        return int(id_val) if id_val else 0
                    except:
                        return 0
                
                sorted_results = sorted(unique_results.values(), key=get_id)
                
                for village in sorted_results:
                    writer.writerow(village)
            
            new_count = len(self.results)
            total_count = len(unique_results)
            added_count = total_count - len(existing_results)
            
            logger.info(f"Результаты сохранены в файл: {filename}")
            logger.info(f"Новых записей: {new_count}")
            logger.info(f"Добавлено уникальных записей: {added_count}")
            logger.info(f"Всего записей в файле: {total_count}")
            
            # Статистика
            target_count = sum(1 for v in unique_results.values() if v.get('Оценка целевого клиента') == 'Целевой')
            phones_count = sum(1 for v in unique_results.values() if v.get('Телефон основной'))
            logger.info(f"Целевых клиентов: {target_count}")
            logger.info(f"Записей с телефонами: {phones_count}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов: {e}")
    
    def run(self, sources: List[str] = None, regions: List[str] = None, max_pages: int = 3):
        """Запуск парсинга из всех источников"""
        if sources is None:
            sources = ['cian', 'domclick', 'yandex', 'cottage', 'poselki', 'avito']
        
        if regions is None:
            regions = ['moskovskaya-oblast']
        
        logger.info("=" * 60)
        logger.info("НАЧАЛО ПАРСИНГА КОТТЕДЖНЫХ ПОСЕЛКОВ РФ")
        logger.info("=" * 60)
        
        all_villages = []
        
        # Парсинг из каждого источника
        for source in sources:
            try:
                if source == 'cian':
                    for region in regions:
                        villages = self.parse_cian(region, max_pages)
                        all_villages.extend(villages)
                
                elif source == 'domclick':
                    for region in regions:
                        villages = self.parse_domclick(region, max_pages)
                        all_villages.extend(villages)
                
                elif source == 'yandex':
                    for region in regions:
                        villages = self.parse_yandex_realty(region, max_pages)
                        all_villages.extend(villages)
                
                elif source == 'cottage':
                    villages = self.parse_cottage_ru(max_pages)
                    all_villages.extend(villages)
                
                elif source == 'poselki':
                    villages = self.parse_poselki_ru(max_pages)
                    all_villages.extend(villages)
                
                elif source == 'avito':
                    for region in regions:
                        villages = self.parse_avito(region, max_pages)
                        all_villages.extend(villages)
                
            except Exception as e:
                logger.error(f"Ошибка при парсинге источника {source}: {e}")
                continue
        
        # Фильтрация и валидация данных
        logger.info(f"Всего найдено записей до валидации: {len(all_villages)}")
        
        validated_villages = []
        for village in all_villages:
            # Валидация названия
            name = village.get('Название поселка', '')
            if not name or not self.validate_village_name(name):
                continue
            
            # Валидация URL
            url = village.get('Ссылка на источник', '')
            if url and not self.is_village_url(url):
                continue
            
            validated_villages.append(village)
        
        logger.info(f"После валидации осталось записей: {len(validated_villages)}")
        
        # Удаление дубликатов по названию и адресу
        unique_villages = {}
        for village in validated_villages:
            # Нормализация названия для сравнения
            name = village.get('Название поселка', '').lower().strip()
            
            # Очистка названия от лишних символов для сравнения
            name_normalized = re.sub(r'[^\w\s]', '', name)
            name_normalized = re.sub(r'\s+', ' ', name_normalized).strip()
            
            address = village.get('Адрес', '').lower().strip() if village.get('Адрес') else ''
            
            # Извлекаем URL для дополнительной проверки
            url = village.get('Ссылка на источник', '')
            
            # Создаем ключ для дедупликации (используем нормализованное название)
            key = (name_normalized, address)
            
            # Альтернативный ключ только по названию (если адреса нет)
            if not address:
                key = (name_normalized,)
            
            if name_normalized:  # Только если есть название
                if key not in unique_villages:
                    unique_villages[key] = village
                else:
                    # Объединение данных из разных источников
                    existing = unique_villages[key]
                    
                    # Выбираем запись с более полными данными
                    existing_score = sum(1 for f in ['Адрес', 'Телефон основной', 'Email', 'Количество домов/участков'] 
                                       if existing.get(f))
                    village_score = sum(1 for f in ['Адрес', 'Телефон основной', 'Email', 'Количество домов/участков'] 
                                      if village.get(f))
                    
                    # Если новая запись более полная, заменяем
                    if village_score > existing_score:
                        unique_villages[key] = village
                    else:
                        # Иначе объединяем данные
                        for field in FIELDS:
                            if not existing.get(field) and village.get(field):
                                existing[field] = village[field]
        
        logger.info(f"После удаления дубликатов осталось записей: {len(unique_villages)}")
        self.results = list(unique_villages.values())
        
        # Обогащение данных из детальных страниц для получения телефонов через Selenium
        logger.info("Обогащение данных из детальных страниц для получения телефонов...")
        enriched_count = 0
        # Лимит обогащения для защиты от зависаний при большом числе записей
        max_enrich = min(MAX_ENRICH, len(self.results))
        
        for i, village in enumerate(self.results):
            if village.get('Ссылка на источник') and self.is_village_url(village['Ссылка на источник']):
                # Пропускаем, если телефон уже есть
                if village.get('Телефон основной'):
                    continue
                if enriched_count >= max_enrich:
                    logger.info(f"Достигнут лимит обогащения ({MAX_ENRICH}). Остальные записи пропущены.")
                    break
                try:
                    enriched_village = self.enrich_village_data(village)
                    # Обновляем запись в списке
                    self.results[i] = enriched_village
                    enriched_count += 1
                    if enriched_count % 5 == 0:
                        logger.info(f"Обогащено записей: {enriched_count}/{len(self.results)}")
                except Exception as e:
                    logger.warning(f"Ошибка при обогащении данных для {village.get('Название поселка')}: {e}")
                    continue
        
        logger.info(f"Обогащение завершено. Обработано записей: {enriched_count}")
        
        logger.info("=" * 60)
        logger.info("ПАРСИНГ ЗАВЕРШЕН")
        logger.info("=" * 60)
        
        # Статистика по телефонам
        total_phones = sum(1 for v in self.results if v.get('Телефон основной'))
        if self.use_selenium:
            logger.info(f"Телефонов получено через Selenium: {self.selenium_phones_count}")
        logger.info(f"Всего записей с телефонами: {total_phones} из {len(self.results)}")
        
        # Закрываем Selenium WebDriver
        if self.selenium_extractor:
            self.selenium_extractor.close()
        
        # Сохранение результатов (дополняем существующий файл)
        self.save_results(append=True)


def main():
    """Главная функция"""
    import argparse
    
    # Парсинг аргументов командной строки
    arg_parser = argparse.ArgumentParser(description='Парсинг информации о коттеджных поселках РФ')
    arg_parser.add_argument('--no-selenium', action='store_true', 
                          help='Отключить использование Selenium для получения телефонов (быстрее, но меньше телефонов)')
    arg_parser.add_argument('--selenium-visible', action='store_true',
                          help='Запустить браузер в видимом режиме (для отладки)')
    arg_parser.add_argument('--sources', nargs='+', 
                          default=['cian'],
                          help='Источники для парсинга: cian, domclick, yandex, cottage, poselki, avito')
    arg_parser.add_argument('--regions', nargs='+',
                          default=['moskovskaya-oblast'],
                          help='Регионы для парсинга')
    arg_parser.add_argument('--max-pages', type=int, default=3,
                          help='Количество страниц для парсинга с каждого источника')
    
    args = arg_parser.parse_args()
    
    # Создание парсера с настройками Selenium
    use_selenium = not args.no_selenium
    selenium_headless = not args.selenium_visible
    
    if use_selenium:
        logger.info("Selenium включен для получения телефонов")
        if not selenium_headless:
            logger.info("Браузер будет запущен в видимом режиме")
    else:
        logger.info("Selenium отключен. Телефоны будут извлекаться только из статического HTML")
    
    parser = VillageParser(use_selenium=use_selenium, selenium_headless=selenium_headless)
    
    # Настройки парсинга
    sources = args.sources
    regions = args.regions
    max_pages = args.max_pages
    
    logger.info(f"Источники: {sources}")
    logger.info(f"Регионы: {regions}")
    logger.info(f"Максимум страниц: {max_pages}")
    
    parser.run(sources=sources, regions=regions, max_pages=max_pages)


if __name__ == '__main__':
    main()
