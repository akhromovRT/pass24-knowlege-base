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


class VillageParser:
    """Класс для парсинга информации о коттеджных поселках"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.results: List[Dict] = []
        self.village_id = 1
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        
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
    
    def parse_cian(self, region: str = 'moskovskaya-oblast', max_pages: int = 5) -> List[Dict]:
        """Парсинг Cian.ru"""
        logger.info(f"Начинаем парсинг Cian.ru для региона: {region}")
        villages = []
        
        try:
            base_url = f"https://www.cian.ru/cat.php?deal_type=sale&object_type%5B0%5D=2&region={region}"
            
            for page in range(1, max_pages + 1):
                self.delay()
                url = f"{base_url}&p={page}"
                
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Поиск карточек поселков (структура может отличаться)
                    cards = soup.find_all(['article', 'div'], class_=re.compile(r'card|item|village', re.I))
                    
                    if not cards:
                        logger.warning(f"Не найдено карточек на странице {page}")
                        break
                    
                    for card in cards:
                        village_data = self._parse_cian_card(card, url)
                        if village_data:
                            villages.append(village_data)
                    
                    logger.info(f"Обработано страниц: {page}/{max_pages}, найдено поселков: {len(villages)}")
                    
                except Exception as e:
                    logger.error(f"Ошибка при парсинге страницы {page} Cian.ru: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге Cian.ru: {e}")
        
        logger.info(f"Всего найдено поселков на Cian.ru: {len(villages)}")
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
            
            # Поиск ссылки на страницу поселка
            link_elem = card.find('a', href=True)
            if link_elem:
                href = link_elem.get('href', '')
                if href.startswith('/'):
                    href = urljoin('https://www.cian.ru', href)
                village['Ссылка на источник'] = href
            
            # Поиск названия
            title_elem = card.find(['h2', 'h3', 'a'], class_=re.compile(r'title|name|heading', re.I))
            if not title_elem:
                title_elem = card.find('a', href=True)
            
            if title_elem:
                village['Название поселка'] = title_elem.get_text(strip=True)
            
            # Поиск адреса
            address_elem = card.find(['div', 'span'], class_=re.compile(r'address|location|geo', re.I))
            if address_elem:
                address_text = address_elem.get_text(strip=True)
                village['Адрес'] = address_text
                
                # Попытка извлечь регион из адреса
                if 'область' in address_text.lower() or 'край' in address_text.lower():
                    parts = address_text.split(',')
                    if parts:
                        village['Регион'] = parts[0].strip()
                        if len(parts) > 1:
                            village['Город/Район'] = parts[1].strip()
            
            # Поиск количества домов
            houses_elem = card.find(text=re.compile(r'дом|участк|коттедж', re.I))
            if houses_elem:
                parent = houses_elem.parent if hasattr(houses_elem, 'parent') else None
                if parent:
                    houses_text = parent.get_text()
                    houses_num = self.extract_number(houses_text)
                    if houses_num:
                        village['Количество домов/участков'] = houses_num
            
            # Поиск телефона
            phone_elem = card.find(['a', 'span'], href=re.compile(r'tel:', re.I))
            if phone_elem:
                phone = phone_elem.get('href', '').replace('tel:', '').strip()
                village['Телефон основной'] = phone
            else:
                # Поиск телефона в тексте
                card_text = card.get_text()
                phone = self.extract_phone(card_text)
                if phone:
                    village['Телефон основной'] = phone
            
            # Поиск email
            email_elem = card.find('a', href=re.compile(r'mailto:', re.I))
            if email_elem:
                email = email_elem.get('href', '').replace('mailto:', '').strip()
                village['Email'] = email
            else:
                card_text = card.get_text()
                email = self.extract_email(card_text)
                if email:
                    village['Email'] = email
            
            # Проверка наличия минимальной информации
            if village['Название поселка']:
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
            
            # Поиск количества домов
            if not village.get('Количество домов/участков'):
                houses_match = re.search(r'(\d+)\s*(?:дом|участк|коттедж)', page_text, re.I)
                if houses_match:
                    village['Количество домов/участков'] = int(houses_match.group(1))
            
            # Поиск информации об ограждении
            if 'огражден' in page_text.lower() or 'забор' in page_text.lower():
                village['Наличие ограждения'] = 'Да'
            
            # Поиск информации о КПП
            if 'кпп' in page_text.lower() or 'контрольно-пропускной' in page_text.lower():
                village['Наличие КПП'] = 'Да'
            
            # Поиск информации об охране
            if 'охрана' in page_text.lower() or 'чоп' in page_text.lower():
                village['Наличие охраны'] = 'Да'
                if 'чоп' in page_text.lower():
                    village['Тип охраны'] = 'ЧОП'
            
            # Поиск телефона
            if not village.get('Телефон основной'):
                phone = self.extract_phone(page_text)
                if phone:
                    village['Телефон основной'] = phone
            
            # Поиск email
            if not village.get('Email'):
                email = self.extract_email(page_text)
                if email:
                    village['Email'] = email
            
            # Поиск сайта
            site_link = soup.find('a', href=re.compile(r'http', re.I))
            if site_link:
                href = site_link.get('href', '')
                if 'cian.ru' not in href and 'domclick.ru' not in href:
                    village['Сайт поселка/УК'] = href
            
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
        
        # Удаление дубликатов по названию и адресу
        unique_villages = {}
        for village in all_villages:
            key = (village.get('Название поселка', ''), village.get('Адрес', ''))
            if key[0]:  # Только если есть название
                if key not in unique_villages:
                    unique_villages[key] = village
                else:
                    # Объединение данных из разных источников
                    existing = unique_villages[key]
                    for field in FIELDS:
                        if not existing.get(field) and village.get(field):
                            existing[field] = village[field]
        
        self.results = list(unique_villages.values())
        
        # Обогащение данных из детальных страниц (опционально, можно отключить для ускорения)
        logger.info("Обогащение данных из детальных страниц...")
        enriched_count = 0
        for village in self.results[:10]:  # Ограничение для примера
            if village.get('Ссылка на источник'):
                village = self.enrich_village_data(village)
                enriched_count += 1
                if enriched_count % 5 == 0:
                    logger.info(f"Обогащено записей: {enriched_count}")
        
        logger.info("=" * 60)
        logger.info("ПАРСИНГ ЗАВЕРШЕН")
        logger.info("=" * 60)
        
        # Сохранение результатов
        self.save_results()


def main():
    """Главная функция"""
    parser = VillageParser()
    
    # Настройки парсинга
    sources = ['cian']  # Можно добавить другие: 'domclick', 'yandex', 'cottage', 'poselki', 'avito'
    regions = ['moskovskaya-oblast']  # Можно добавить другие регионы
    max_pages = 3  # Количество страниц для парсинга с каждого источника
    
    parser.run(sources=sources, regions=regions, max_pages=max_pages)


if __name__ == '__main__':
    main()
