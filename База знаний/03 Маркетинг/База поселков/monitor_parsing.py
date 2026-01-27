#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Скрипт мониторинга выполнения парсинга"""

import time
import subprocess
import re
from datetime import datetime
import os

def get_status():
    """Получает текущий статус выполнения"""
    # Проверка процесса
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        is_running = 'parse_cottage_villages.py' in result.stdout
    except:
        is_running = False
    
    # Подсчет из лога
    log_files = ['/tmp/parsing_restarted.log', '/tmp/parsing_fixed.log', '/tmp/parsing_output.log']
    log_file = None
    
    for lf in log_files:
        if os.path.exists(lf):
            log_file = lf
            break
    
    phones = 0
    attempts = 0
    
    if log_file:
        try:
            with open(log_file, 'r') as f:
                content = f.read()
                phones = len(set(re.findall(r'Найден телефон: (\+7\d{10})', content)))
                attempts = len(re.findall(r'Найден телефон:', content))
        except:
            pass
    
    return is_running, phones, attempts, log_file

def main():
    print("=" * 70)
    print("📊 МОНИТОРИНГ СКРИПТА ПАРСИНГА")
    print("=" * 70)
    print("Обновление каждые 10 секунд...\n")
    
    iteration = 0
    start_time = time.time()
    last_attempts = 0
    TOTAL_ESTIMATED = 40  # 2 страницы × ~20 поселков
    
    while True:
        iteration += 1
        is_running, phones, attempts, log_file = get_status()
        
        elapsed = time.time() - start_time
        
        # Расчет прогресса
        progress = min(100, (attempts / TOTAL_ESTIMATED) * 100) if TOTAL_ESTIMATED > 0 else 0
        
        # Оценка оставшегося времени
        if attempts > 0 and attempts > last_attempts:
            avg_time = elapsed / attempts
            remaining = max(0, TOTAL_ESTIMATED - attempts)
            remaining_time = remaining * avg_time
        elif attempts > 0:
            # Используем последнюю оценку
            avg_time = elapsed / attempts if attempts > 0 else 0
            remaining = max(0, TOTAL_ESTIMATED - attempts)
            remaining_time = remaining * avg_time if avg_time > 0 else 0
        else:
            remaining_time = 0
        
        # Индикатор активности
        if attempts > last_attempts:
            activity = "🟢 Активен"
        elif attempts == last_attempts and attempts > 0:
            activity = "🟡 Ожидание"
        else:
            activity = "⚪ Старт"
        
        now = datetime.now().strftime('%H:%M:%S')
        status_text = "🟢 РАБОТАЕТ" if is_running else "🔴 ЗАВЕРШЕН"
        
        # Очистка экрана (опционально, можно закомментировать)
        # print("\033[2J\033[H", end="")
        
        print(f"[{now}] Проверка #{iteration}")
        print(f"  Статус: {status_text}")
        print(f"  📊 Прогресс: {progress:.1f}% ({attempts}/{TOTAL_ESTIMATED} попыток)")
        print(f"  📞 Найдено телефонов: {phones} уникальных")
        print(f"  ⏱️  Время работы: {elapsed/60:.1f} минут")
        if remaining_time > 0:
            print(f"  ⏳ Осталось: ~{remaining_time/60:.1f} минут ({remaining_time:.0f} секунд)")
        else:
            print(f"  ⏳ Осталось: рассчитывается...")
        print(f"  {activity}")
        if log_file:
            print(f"  📄 Лог: {log_file}")
        print()
        
        last_attempts = attempts
        
        if not is_running and iteration > 1:
            print("=" * 70)
            print("✅ СКРИПТ ЗАВЕРШИЛСЯ!")
            print("=" * 70)
            print(f"⏱️  Общее время: {elapsed/60:.1f} минут")
            print(f"📞 Найдено уникальных телефонов: {phones}")
            print(f"🔄 Всего попыток извлечения: {attempts}")
            print(f"📄 Проверьте CSV файл для результатов")
            break
        
        time.sleep(10)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nМониторинг прерван пользователем")
