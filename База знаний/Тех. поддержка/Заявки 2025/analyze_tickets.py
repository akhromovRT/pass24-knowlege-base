#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа заявок технической поддержки за 2025 год
Выявляет типичные запросы и формирует базу знаний
"""

import csv
import re
from collections import Counter, defaultdict
from datetime import datetime
import json

def read_csv_file(filename):
    """Читает CSV файл с заявками"""
    tickets = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            tickets.append(row)
    return tickets

def categorize_ticket(description, title, service):
    """Категоризирует заявку по описанию"""
    if not description:
        description = ""
    if not title:
        title = ""
    
    text = (description + " " + title).lower()
    
    # Категории проблем
    categories = {
        "Регистрация и вход": [
            r"не могу зарегистрироваться", r"не могу зайти", r"не могу войти",
            r"не приходит.*код", r"не приходит.*смс", r"смс.*не приходит",
            r"восстановить пароль", r"сбросить пароль", r"забыл пароль",
            r"invalid phone token", r"ошибка сброса пароля", r"ошибка отправки кода",
            r"вылетело приложение", r"разлогинилось", r"слетел пароль",
            r"не принимает пароль", r"неверный пароль", r"не подходит пароль"
        ],
        "Распознавание номеров": [
            r"камера.*не считывает", r"не распознает.*номер", r"не считывает.*номер",
            r"номер.*не распознается", r"камера.*не реагирует", r"не срабатывает.*распознавание",
            r"путает.*цифры", r"некорректное распознавание", r"некорректно.*распознает"
        ],
        "Пропуска": [
            r"не могу.*создать.*пропуск", r"не могу.*заказать.*пропуск",
            r"пропуск.*не срабатывает", r"шлагбаум.*не открывается",
            r"не открылся.*шлагбаум", r"проблема.*с пропуском",
            r"ошибка.*при заказе.*пропуска", r"не работает.*пропуск"
        ],
        "Оборудование": [
            r"не работает.*камера", r"камера.*не работает",
            r"не работает.*турникет", r"не работает.*считыватель",
            r"проблема.*с оборудованием", r"неисправность"
        ],
        "Приложение": [
            r"приложение.*вылетает", r"приложение.*не работает",
            r"ошибка.*в приложении", r"не работает.*функционал",
            r"приложение.*закрывается", r"вылетает.*приложение"
        ],
        "Доверенные лица": [
            r"не могу.*добавить.*доверен", r"доверенное лицо",
            r"проблема.*с доверен", r"регистрация.*доверен"
        ],
        "Добавление объектов": [
            r"не могу.*добавить.*объект", r"как.*добавить.*объект",
            r"номер.*объекта", r"идентификатор.*объекта"
        ],
        "Консультации": [
            r"как.*создать", r"как.*добавить", r"как.*узнать",
            r"подскажите", r"возможно ли", r"каким образом"
        ]
    }
    
    for category, patterns in categories.items():
        for pattern in patterns:
            if re.search(pattern, text):
                return category
    
    return "Другое"

def analyze_tickets(tickets):
    """Анализирует заявки и формирует статистику"""
    stats = {
        "total": len(tickets),
        "by_category": Counter(),
        "by_status": Counter(),
        "by_service": Counter(),
        "by_source": Counter(),
        "typical_requests": defaultdict(list),
        "avg_response_time": [],
        "avg_resolution_time": []
    }
    
    for ticket in tickets:
        # Категоризация
        description = ticket.get("Поля форм", "") or ticket.get("Название заявки", "")
        title = ticket.get("Название заявки", "")
        service = ticket.get("Услуга", "")
        
        category = categorize_ticket(description, title, service)
        stats["by_category"][category] += 1
        
        # Статусы
        status = ticket.get("Статус", "")
        stats["by_status"][status] += 1
        
        # Услуги
        stats["by_service"][service] += 1
        
        # Источники
        source = ticket.get("Источник", "")
        stats["by_source"][source] += 1
        
        # Типичные запросы
        if description and len(description) > 10:
            stats["typical_requests"][category].append({
                "id": ticket.get("ID", ""),
                "description": description[:200],
                "status": status
            })
        
        # Время обработки
        response_time = ticket.get("Время до принятия", "")
        resolution_time = ticket.get("Время до завершения", "")
        
        # Парсинг времени (упрощенный)
        if response_time and "<" not in response_time:
            try:
                # Пример: " 3 мин." или "1 ч. 11 м."
                if "мин" in response_time:
                    minutes = re.search(r'(\d+)\s*мин', response_time)
                    if minutes:
                        stats["avg_response_time"].append(int(minutes.group(1)))
            except:
                pass
    
    return stats

def generate_knowledge_base(stats):
    """Генерирует структуру базы знаний"""
    kb_structure = {}
    
    for category, count in stats["by_category"].most_common():
        if count > 10:  # Только категории с более чем 10 заявками
            kb_structure[category] = {
                "count": count,
                "percentage": round(count / stats["total"] * 100, 1),
                "typical_requests": stats["typical_requests"][category][:10]  # Топ 10
            }
    
    return kb_structure

if __name__ == "__main__":
    filename = "завки в ТП 2025.csv"
    tickets = read_csv_file(filename)
    
    print(f"Всего заявок: {len(tickets)}")
    
    stats = analyze_tickets(tickets)
    
    print("\n=== Статистика по категориям ===")
    for category, count in stats["by_category"].most_common():
        percentage = round(count / stats["total"] * 100, 1)
        print(f"{category}: {count} ({percentage}%)")
    
    print("\n=== Статистика по статусам ===")
    for status, count in stats["by_status"].most_common():
        print(f"{status}: {count}")
    
    print("\n=== Статистика по источникам ===")
    for source, count in stats["by_source"].most_common():
        print(f"{source}: {count}")
    
    # Генерация базы знаний
    kb = generate_knowledge_base(stats)
    
    # Сохранение результатов
    with open("analysis_results.json", "w", encoding="utf-8") as f:
        json.dump({
            "stats": {k: dict(v) if isinstance(v, Counter) else v for k, v in stats.items()},
            "knowledge_base": kb
        }, f, ensure_ascii=False, indent=2)
    
    print("\nАнализ завершен. Результаты сохранены в analysis_results.json")
