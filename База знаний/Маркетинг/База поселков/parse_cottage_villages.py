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
            self.driver.set_page_load_timeout(30)
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
        self.selenium_phones_count = 0  # Счетчик телефонов, полученных через Selenium
        
        if self.use_selenium:
            try:
                self.selenium_extractor = SeleniumPhoneExtractor(headless=selenium_headless)
            except Exception as e:
                logger.warning(f"Не удалось инициализировать Selenium: {e}. Парсинг будет работать без Selenium.")
                self.use_selenium = False
        
    def delay(self):
        """Случайная задержка между запросами"""
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
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
            if not village.get('Телефон основной') and village.get('Ссылка на источник') and self.use_selenium and self.selenium_extractor:
                try:
                    logger.debug(f"Попытка получить телефон через Selenium для {village.get('Название поселка')}")
                    # Добавляем задержку перед запросом через Selenium
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
        """Парсинг DomClick.ru"""
        logger.info(f"Начинаем парсинг DomClick.ru для региона: {region}")
        villages = []
        
        try:
            base_url = "https://domclick.ru/search"
            # DomClick может использовать API или другой формат URL
            
            # Здесь должна быть логика парсинга DomClick
            # Структура может отличаться от Cian
            
            logger.info(f"Всего найдено поселков на DomClick.ru: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге DomClick.ru: {e}")
        
        return villages
    
    def parse_yandex_realty(self, region: str = 'moskovskaya-oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг Яндекс.Недвижимость"""
        logger.info(f"Начинаем парсинг Яндекс.Недвижимость для региона: {region}")
        villages = []
        
        try:
            base_url = "https://realty.yandex.ru/moskva_i_mo/kupit/uchastok/"
            # Яндекс.Недвижимость может использовать динамический контент
            
            logger.info(f"Всего найдено поселков на Яндекс.Недвижимость: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Яндекс.Недвижимость: {e}")
        
        return villages
    
    def parse_cottage_ru(self, max_pages: int = 5) -> List[Dict]:
        """Парсинг Cottage.ru"""
        logger.info("Начинаем парсинг Cottage.ru")
        villages = []
        
        try:
            base_url = "https://cottage.ru/poselki"
            
            logger.info(f"Всего найдено поселков на Cottage.ru: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Cottage.ru: {e}")
        
        return villages
    
    def parse_poselki_ru(self, max_pages: int = 5) -> List[Dict]:
        """Парсинг Poselki.ru"""
        logger.info("Начинаем парсинг Poselki.ru")
        villages = []
        
        try:
            base_url = "https://poselki.ru"
            
            logger.info(f"Всего найдено поселков на Poselki.ru: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Poselki.ru: {e}")
        
        return villages
    
    def parse_avito(self, region: str = 'moskovskaya_oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг Авито Недвижимость"""
        logger.info(f"Начинаем парсинг Авито для региона: {region}")
        villages = []
        
        try:
            base_url = f"https://www.avito.ru/{region}/zemelnye_uchastki/kottedzhnye_poselki"
            
            logger.info(f"Всего найдено поселков на Авито: {len(villages)}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге Авито: {e}")
        
        return villages
    
    def enrich_village_data(self, village: Dict) -> Dict:
        """Дополнение данных о поселке из детальной страницы"""
        if not village.get('Ссылка на источник'):
            return village
        
        try:
            self.delay()
            response = self.session.get(village['Ссылка на источник'], timeout=30)
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
                if not village.get('Телефон основной') and self.use_selenium and self.selenium_extractor:
                    try:
                        logger.debug(f"Попытка получить телефон через Selenium для {village.get('Название поселка')}")
                        # Добавляем задержку перед запросом через Selenium
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
            if village['Количество домов/участков'] >= 20:
                score += 1
        
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
    
    def save_results(self, filename: str = 'Результат парсинга информации в интернете.csv'):
        """Сохранение результатов в CSV файл"""
        if not self.results:
            logger.warning("Нет данных для сохранения")
            return
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
                writer.writeheader()
                
                for village in self.results:
                    # Оценка целевого клиента перед сохранением
                    village['Оценка целевого клиента'] = self.assess_target_client(village)
                    writer.writerow(village)
            
            logger.info(f"Результаты сохранены в файл: {filename}")
            logger.info(f"Всего записей: {len(self.results)}")
            
            # Статистика
            target_count = sum(1 for v in self.results if v.get('Оценка целевого клиента') == 'Целевой')
            logger.info(f"Целевых клиентов: {target_count}")
            
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
        # Увеличиваем лимит обогащения для получения большего количества телефонов
        max_enrich = len(self.results)  # Обогащаем все записи для получения телефонов
        
        for i, village in enumerate(self.results):
            if village.get('Ссылка на источник') and self.is_village_url(village['Ссылка на источник']):
                # Пропускаем, если телефон уже есть
                if village.get('Телефон основной'):
                    continue
                    
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
        
        # Сохранение результатов
        self.save_results()


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
