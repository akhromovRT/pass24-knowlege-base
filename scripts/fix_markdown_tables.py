#!/usr/bin/env python3
"""
Скрипт для массового исправления таблиц Markdown в базе знаний.
Исправляет длинные разделители таблиц на короткие стандартные.

Использование:
    python3 scripts/fix_markdown_tables.py [путь_к_файлу_или_директории]
    
Примеры:
    # Исправить один файл
    python3 scripts/fix_markdown_tables.py "База знаний/ОС/Обучение/Файл.md"
    
    # Исправить все файлы в директории
    python3 scripts/fix_markdown_tables.py "База знаний"
    
    # Исправить все файлы в базе знаний (без аргументов)
    python3 scripts/fix_markdown_tables.py
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple


def fix_table_separator(line: str) -> str:
    """
    Исправляет длинный разделитель таблицы на короткий стандартный.
    
    Args:
        line: Строка с разделителем таблицы
        
    Returns:
        Исправленная строка
    """
    # Проверяем, является ли строка разделителем таблицы
    if not re.match(r'^\|[\s\-|]+\|', line):
        return line
    
    # Подсчитываем количество колонок (количество |)
    columns = line.count('|') - 1  # Минус 1, так как первый и последний | - границы
    
    if columns <= 0:
        return line
    
    # Создаем стандартный разделитель: | --- | --- | ... |
    separator = '| ' + ' | '.join(['---'] * columns) + ' |'
    
    return separator


def fix_tables_in_content(content: str) -> Tuple[str, int]:
    """
    Исправляет все таблицы в содержимом файла.
    
    Args:
        content: Содержимое файла
        
    Returns:
        Кортеж (исправленное содержимое, количество исправленных таблиц)
    """
    lines = content.split('\n')
    fixed_lines = []
    fixed_count = 0
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Проверяем, является ли строка началом таблицы (содержит |)
        if '|' in line and re.match(r'^\|.*\|.*\|', line):
            # Это может быть заголовок таблицы
            fixed_lines.append(line)
            i += 1
            
            # Проверяем следующую строку - это должен быть разделитель
            if i < len(lines):
                next_line = lines[i]
                
                # Если это разделитель с длинными дефисами
                if re.match(r'^\|[\s\-]{20,}\|', next_line):
                    original = next_line
                    fixed = fix_table_separator(next_line)
                    
                    if original != fixed:
                        fixed_count += 1
                        fixed_lines.append(fixed)
                    else:
                        fixed_lines.append(next_line)
                    
                    i += 1
                else:
                    # Не разделитель, просто добавляем
                    fixed_lines.append(next_line)
                    i += 1
        else:
            fixed_lines.append(line)
            i += 1
    
    return '\n'.join(fixed_lines), fixed_count


def process_file(file_path: Path, dry_run: bool = False) -> Tuple[bool, int]:
    """
    Обрабатывает один файл.
    
    Args:
        file_path: Путь к файлу
        dry_run: Если True, только показывает что будет исправлено, не сохраняет
        
    Returns:
        Кортеж (успешно ли обработан, количество исправленных таблиц)
    """
    try:
        # Читаем файл
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Исправляем таблицы
        fixed_content, fixed_count = fix_tables_in_content(content)
        
        if fixed_count > 0:
            if not dry_run:
                # Сохраняем исправленный файл
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(fixed_content)
                print(f"✅ Исправлено {fixed_count} таблиц в: {file_path}")
            else:
                print(f"🔍 Найдено {fixed_count} таблиц для исправления в: {file_path}")
            
            return True, fixed_count
        else:
            return True, 0
            
    except Exception as e:
        print(f"❌ Ошибка при обработке {file_path}: {e}", file=sys.stderr)
        return False, 0


def find_markdown_files(directory: Path) -> List[Path]:
    """
    Находит все Markdown файлы в директории рекурсивно.
    
    Args:
        directory: Директория для поиска
        
    Returns:
        Список путей к Markdown файлам
    """
    markdown_files = []
    
    for file_path in directory.rglob('*.md'):
        # Пропускаем файлы в .git и других служебных директориях
        if '.git' not in file_path.parts and '.cursor' not in file_path.parts:
            markdown_files.append(file_path)
    
    return sorted(markdown_files)


def main():
    """Главная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Исправляет длинные разделители таблиц Markdown на стандартные короткие'
    )
    parser.add_argument(
        'path',
        nargs='?',
        default='База знаний',
        help='Путь к файлу или директории (по умолчанию: "База знаний")'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Показать что будет исправлено, не сохранять изменения'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Показать подробную информацию'
    )
    
    args = parser.parse_args()
    
    # Определяем путь
    base_path = Path(args.path)
    
    if not base_path.exists():
        print(f"❌ Путь не найден: {base_path}", file=sys.stderr)
        sys.exit(1)
    
    # Определяем файлы для обработки
    if base_path.is_file():
        files = [base_path]
    else:
        files = find_markdown_files(base_path)
    
    if not files:
        print(f"❌ Markdown файлы не найдены в: {base_path}")
        sys.exit(1)
    
    print(f"📋 Найдено {len(files)} файлов для обработки")
    if args.dry_run:
        print("🔍 Режим проверки (dry-run): изменения не будут сохранены")
    print()
    
    # Обрабатываем файлы
    total_fixed = 0
    processed = 0
    errors = 0
    
    for file_path in files:
        success, fixed_count = process_file(file_path, dry_run=args.dry_run)
        
        if success:
            processed += 1
            total_fixed += fixed_count
        else:
            errors += 1
    
    # Итоговая статистика
    print()
    print("=" * 60)
    print(f"📊 Итоговая статистика:")
    print(f"   Обработано файлов: {processed}")
    print(f"   Исправлено таблиц: {total_fixed}")
    if errors > 0:
        print(f"   Ошибок: {errors}")
    print("=" * 60)
    
    if args.dry_run:
        print()
        print("💡 Для применения изменений запустите скрипт без флага --dry-run")


if __name__ == '__main__':
    main()
