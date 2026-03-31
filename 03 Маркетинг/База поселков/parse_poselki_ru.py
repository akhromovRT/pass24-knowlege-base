#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для парсинга информации о коттеджных поселках РФ
с сайта Poselki.ru

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
from urllib.parse import urljoin, urlparse
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
        logging.FileHandler('parsing_poselki_ru.log', encoding='utf-8'),
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
MAX_SELENIUM_PHONE_ATTEMPTS = 60
PAGE_LOAD_TIMEOUT = 20

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
        """Инициализация Selenium WebDriver"""
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
            logger.info("Парсинг будет продолжен без Selenium")
            self.driver = None
    
    def extract_phone_from_url(self, url: str, wait_time: int = 5) -> Optional[str]:
        """Извлечение телефона со страницы с использованием Selenium"""
        if not self.driver or not url:
            return None
        
        try:
            logger.debug(f"Открываем страницу с Selenium: {url}")
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # Поиск телефонов в различных местах
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
            
            # Стратегия 3: Поиск в тексте страницы
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


class PoselkiRuParser:
    """Класс для парсинга информации о коттеджных поселках с Poselki.ru"""
    
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
        self.selenium_phones_count = 0
        self.selenium_phone_attempts = 0
        
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
        """Проверка лимита попыток получения телефона через Selenium"""
        if self.selenium_phone_attempts >= MAX_SELENIUM_PHONE_ATTEMPTS:
            logger.info(f"Достигнут лимит попыток Selenium ({MAX_SELENIUM_PHONE_ATTEMPTS}). Пропуск получения телефонов.")
            return False
        self.selenium_phone_attempts += 1
        return True
    
    def extract_phone(self, text: str) -> Optional[str]:
        """Извлечение телефона из текста"""
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
                phone = match.group(0).strip()
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
            r'^\d+$',
            r'^[а-яё]{1,2}$',
            r'коттеджные\s*посёлки\s*в\s*',
            r'коттеджные\s*поселки\s*в\s*',
            r'поселки\s*в\s*области',
            r'коттеджные\s*поселки$',
        ]
        
        name_lower = name.lower().strip()
        
        for pattern in invalid_patterns:
            if re.search(pattern, name_lower, re.I):
                return False
        
        if len(name.strip()) < 5:
            return False
        
        if name_lower in ['коттеджные посёлки', 'коттеджные поселки', 'поселки', 'кп']:
            return False
        
        return True
    
    def parse_poselki_ru(self) -> List[Dict]:
        """
        Парсинг главной страницы Poselki.ru и страниц отдельных поселков
        
        На сайте Poselki.ru представлены несколько поселков с ссылками на их сайты:
        - Верба Парк (верба-парк.рф)
        - Марьина Гора (марьина-гора.рф)
        - Золотые Сосны (zolotye-sosny.ru)
        """
        logger.info("Начинаем парсинг Poselki.ru")
        villages = []
        
        try:
            base_url = "https://poselki.ru"
            
            # Парсим главную страницу
            self.delay()
            try:
                response = self.session.get(base_url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                logger.info(f"Загружена главная страница Poselki.ru")
                
                # Ищем блоки с поселками
                # На сайте поселки представлены в виде карточек с ссылками
                village_blocks = []
                
                # Ищем ссылки на сайты поселков
                all_links = soup.find_all('a', href=True)
                village_links = []
                
                for link in all_links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # Ищем ссылки на сайты поселков (домены .рф или известные домены)
                    if any(domain in href for domain in ['.рф', 'zolotye-sosny.ru', 'верба-парк', 'марьина-гора']):
                        village_links.append({
                            'url': href,
                            'text': text,
                            'parent': link.find_parent(['div', 'article', 'section', 'li'])
                        })
                
                logger.info(f"Найдено ссылок на поселки: {len(village_links)}")
                
                # Обрабатываем найденные ссылки
                processed_urls = set()
                
                for link_info in village_links:
                    village_url = link_info['url']
                    
                    # Нормализуем URL
                    if not village_url.startswith('http'):
                        if village_url.startswith('//'):
                            village_url = 'https:' + village_url
                        elif village_url.startswith('/'):
                            village_url = urljoin(base_url, village_url)
                        else:
                            village_url = 'https://' + village_url
                    
                    if village_url in processed_urls:
                        continue
                    processed_urls.add(village_url)
                    
                    # Парсим информацию о поселке
                    parent = link_info.get('parent')
                    if parent:
                        village_data = self._parse_village_from_block(parent, village_url, base_url)
                    else:
                        village_data = self._parse_village_from_url(village_url)
                    
                    if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                        villages.append(village_data)
                        logger.info(f"Добавлен поселок: {village_data.get('Название поселка')}")
                
                # Если не нашли через ссылки, ищем карточки поселков на главной странице
                if not villages:
                    # Ищем секции с поселками
                    sections = soup.find_all(['section', 'div'], class_=re.compile(r'village|poselok|item|card|descr', re.I))
                    
                    for section in sections:
                        section_text = section.get_text()
                        if any(keyword in section_text.lower() for keyword in ['поселок', 'коттеджный', 'кп']):
                            village_data = self._parse_village_from_block(section, None, base_url)
                            if village_data and self.validate_village_name(village_data.get('Название поселка', '')):
                                villages.append(village_data)
                
                logger.info(f"Всего найдено поселков на Poselki.ru: {len(villages)}")
                
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут при загрузке главной страницы Poselki.ru")
            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка сети при парсинге Poselki.ru: {e}")
            except Exception as e:
                logger.error(f"Ошибка при парсинге Poselki.ru: {e}")
        
        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге Poselki.ru: {e}")
        
        return villages
    
    def _parse_village_from_block(self, block, village_url: Optional[str], source_url: str) -> Optional[Dict]:
        """Парсинг информации о поселке из блока на главной странице"""
        try:
            village = self._create_empty_village()
            village['Ссылка на источник'] = source_url
            
            block_text = block.get_text()
            
            # Поиск названия поселка
            # Ищем заголовки
            title_elem = block.find(['h1', 'h2', 'h3', 'h4', 'h5'])
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if self.validate_village_name(title_text):
                    village['Название поселка'] = title_text
            
            # Если не нашли в заголовке, ищем в тексте
            if not village.get('Название поселка'):
                # Ищем ссылки с названиями
                links = block.find_all('a', href=True)
                for link in links:
                    link_text = link.get_text(strip=True)
                    href = link.get('href', '')
                    
                    # Если это ссылка на сайт поселка
                    if any(domain in href for domain in ['.рф', 'zolotye-sosny.ru', 'верба-парк', 'марьина-гора']):
                        if self.validate_village_name(link_text):
                            village['Название поселка'] = link_text
                            if not village_url:
                                village_url = href
                            break
            
            # Если все еще не нашли, ищем в тексте блока
            if not village.get('Название поселка'):
                text_lines = [line.strip() for line in block_text.split('\n') if line.strip()]
                for line in text_lines[:15]:
                    line_clean = re.sub(r'[^\w\s\-]', '', line).strip()
                    if self.validate_village_name(line_clean) and len(line_clean) > 5:
                        village['Название поселка'] = line_clean
                        break
            
            # Сохраняем ссылку на сайт поселка
            if village_url:
                village['Сайт поселка/УК'] = village_url
                # Если это ссылка на внешний сайт, используем её как источник
                if village_url != source_url:
                    village['Ссылка на источник'] = village_url
            
            # Поиск телефона
            phone = self.extract_phone(block_text)
            if phone:
                village['Телефон основной'] = phone
            
            # Поиск email
            email = self.extract_email(block_text)
            if email:
                village['Email'] = email
            
            # Поиск региона (обычно упоминается "Подмосковье" или "Московская область")
            if 'подмосков' in block_text.lower() or 'московск' in block_text.lower():
                village['Регион'] = 'Московская область'
            
            # Поиск информации о характеристиках
            if 'заселен' in block_text.lower() or 'построен' in block_text.lower():
                village['Статус поселка'] = 'Построен и заселен'
            elif 'строительств' in block_text.lower():
                village['Статус поселка'] = 'В строительстве (>80%)'
            
            # Поиск информации об охране и КПП
            if any(keyword in block_text.lower() for keyword in ['кпп', 'контрольно-пропускной', 'пропускной пункт']):
                village['Наличие КПП'] = 'Да'
            
            if any(keyword in block_text.lower() for keyword in ['охрана', 'охраняется', 'чоп']):
                village['Наличие охраны'] = 'Да'
            
            if any(keyword in block_text.lower() for keyword in ['огражден', 'ограждение', 'забор']):
                village['Наличие ограждения'] = 'Да'
            
            # Если есть ссылка на сайт поселка, парсим его для получения дополнительной информации
            if village.get('Сайт поселка/УК') and village.get('Сайт поселка/УК') != source_url:
                self.delay()
                additional_info = self._parse_village_website(village['Сайт поселка/УК'])
                if additional_info:
                    # Объединяем информацию
                    for key, value in additional_info.items():
                        if value and not village.get(key):
                            village[key] = value
            
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
            
            if not village.get('Название поселка') or not self.validate_village_name(village.get('Название поселка', '')):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге блока поселка: {e}")
            return None
    
    def _parse_village_from_url(self, url: str) -> Optional[Dict]:
        """Парсинг информации о поселке по URL"""
        try:
            village = self._create_empty_village()
            village['Ссылка на источник'] = url
            village['Сайт поселка/УК'] = url
            
            self.delay()
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()
            
            # Поиск названия поселка
            title_elem = soup.find(['h1', 'h2', 'h3'], class_=re.compile(r'title|heading', re.I))
            if not title_elem:
                title_elem = soup.find('h1')
            
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if self.validate_village_name(title_text):
                    village['Название поселка'] = title_text
            
            # Если не нашли, ищем в title страницы
            if not village.get('Название поселка'):
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    if self.validate_village_name(title_text):
                        village['Название поселка'] = title_text
            
            # Поиск телефона
            phone = self.extract_phone(page_text)
            if phone:
                village['Телефон основной'] = phone
            
            # Поиск email
            email = self.extract_email(page_text)
            if email:
                village['Email'] = email
            
            # Поиск региона
            if 'подмосков' in page_text.lower() or 'московск' in page_text.lower():
                village['Регион'] = 'Московская область'
            
            # Поиск адреса
            address_elem = soup.find(['div', 'span'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                village['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск информации о характеристиках
            if 'заселен' in page_text.lower() or 'построен' in page_text.lower():
                village['Статус поселка'] = 'Построен и заселен'
            elif 'строительств' in page_text.lower():
                village['Статус поселка'] = 'В строительстве (>80%)'
            
            # Поиск информации об охране и КПП
            if any(keyword in page_text.lower() for keyword in ['кпп', 'контрольно-пропускной', 'пропускной пункт']):
                village['Наличие КПП'] = 'Да'
            
            if any(keyword in page_text.lower() for keyword in ['охрана', 'охраняется', 'чоп']):
                village['Наличие охраны'] = 'Да'
            
            if any(keyword in page_text.lower() for keyword in ['огражден', 'ограждение', 'забор']):
                village['Наличие ограждения'] = 'Да'
            
            # Поиск количества домов
            houses_match = re.search(r'(\d+)\s*(дом|участок|коттедж)', page_text, re.I)
            if houses_match:
                village['Количество домов/участков'] = self.extract_number(houses_match.group(0))
            
            # Если телефон не найден, используем Selenium
            if not village.get('Телефон основной') and self.use_selenium and self.selenium_extractor and self._can_do_selenium_phone():
                try:
                    self.delay()
                    phone = self.selenium_extractor.extract_phone_from_url(url)
                    if phone:
                        village['Телефон основной'] = phone
                        self.selenium_phones_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка при получении телефона через Selenium: {e}")
            
            if not village.get('Название поселка') or not self.validate_village_name(village.get('Название поселка', '')):
                return None
            
            self.village_id += 1
            return village
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге поселка по URL {url}: {e}")
            return None
    
    def _parse_village_website(self, url: str) -> Optional[Dict]:
        """Парсинг дополнительной информации со сайта поселка"""
        try:
            self.delay()
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()
            
            info = {}
            
            # Поиск телефона
            phone = self.extract_phone(page_text)
            if phone:
                info['Телефон основной'] = phone
            
            # Поиск email
            email = self.extract_email(page_text)
            if email:
                info['Email'] = email
            
            # Поиск адреса
            address_elem = soup.find(['div', 'span'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                info['Адрес'] = address_elem.get_text(strip=True)
            
            # Поиск региона
            if 'подмосков' in page_text.lower() or 'московск' in page_text.lower():
                info['Регион'] = 'Московская область'
            
            # Поиск количества домов
            houses_match = re.search(r'(\d+)\s*(дом|участок|коттедж)', page_text, re.I)
            if houses_match:
                info['Количество домов/участков'] = self.extract_number(houses_match.group(0))
            
            return info if info else None
            
        except Exception as e:
            logger.debug(f"Ошибка при парсинге сайта поселка {url}: {e}")
            return None
    
    def _create_empty_village(self) -> Dict:
        """Создание пустой записи поселка с дефолтными значениями"""
        return {
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
    
    def save_to_csv(self, filename: str = 'Результат парсинга информации в интернете.csv'):
        """Сохранение результатов в CSV файл"""
        if not self.results:
            logger.warning("Нет данных для сохранения")
            return
        
        # Проверяем, существует ли файл
        file_exists = os.path.exists(filename)
        
        # Читаем существующие данные, если файл есть
        existing_data = []
        existing_ids = set()
        
        if file_exists:
            try:
                with open(filename, 'r', encoding='utf-8-sig', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_data.append(row)
                        if row.get('ID'):
                            try:
                                existing_ids.add(int(row['ID']))
                            except ValueError:
                                pass
            except Exception as e:
                logger.warning(f"Ошибка при чтении существующего файла: {e}")
        
        # Обновляем ID для новых записей
        max_id = max(existing_ids) if existing_ids else 0
        for village in self.results:
            if village.get('ID'):
                try:
                    current_id = int(village['ID'])
                    if current_id <= max_id:
                        max_id += 1
                        village['ID'] = max_id
                except ValueError:
                    max_id += 1
                    village['ID'] = max_id
            else:
                max_id += 1
                village['ID'] = max_id
        
        # Объединяем данные
        all_data = existing_data + self.results
        
        # Сохраняем в файл
        try:
            with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(all_data)
            
            logger.info(f"Сохранено {len(self.results)} новых записей в файл {filename}")
            logger.info(f"Всего записей в файле: {len(all_data)}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в CSV: {e}")
    
    def close(self):
        """Закрытие ресурсов"""
        if self.selenium_extractor:
            self.selenium_extractor.close()


def main():
    """Главная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Парсинг коттеджных поселков с Poselki.ru')
    parser.add_argument('--no-selenium', action='store_true', help='Не использовать Selenium для получения телефонов')
    parser.add_argument('--selenium-headless', action='store_true', default=True, help='Запуск Selenium в headless режиме')
    parser.add_argument('--output', default='Результат парсинга информации в интернете.csv', help='Имя выходного CSV файла')
    
    args = parser.parse_args()
    
    parser_obj = PoselkiRuParser(use_selenium=not args.no_selenium, selenium_headless=args.selenium_headless)
    
    try:
        # Парсим Poselki.ru
        villages = parser_obj.parse_poselki_ru()
        parser_obj.results = villages
        
        # Сохраняем результаты
        parser_obj.save_to_csv(args.output)
        
        logger.info(f"Парсинг завершен. Найдено поселков: {len(villages)}")
        if parser_obj.selenium_phones_count > 0:
            logger.info(f"Телефонов получено через Selenium: {parser_obj.selenium_phones_count}")
        
    except KeyboardInterrupt:
        logger.info("Парсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        parser_obj.close()


if __name__ == '__main__':
    main()
