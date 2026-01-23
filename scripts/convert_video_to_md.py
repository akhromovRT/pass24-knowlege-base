#!/usr/bin/env python3
"""
Скрипт для конвертации видео файлов в Markdown документы с автоматическим
распознаванием речи и извлечением скриншотов из ключевых моментов.

Автор: DimaTorzok
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

try:
    import whisper
except ImportError:
    print("Ошибка: библиотека whisper не установлена.")
    print("Установите её командой: pip install openai-whisper")
    sys.exit(1)


def check_ffmpeg():
    """Проверяет наличие ffmpeg в системе."""
    try:
        subprocess.run(['ffmpeg', '-version'], 
                      capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def get_video_duration(video_path):
    """Получает длительность видео в секундах."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 
             'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', 
             str(video_path)],
            capture_output=True,
            text=True,
            check=True
        )
        return float(result.stdout.strip())
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        return None


def extract_screenshots(video_path, screenshots_dir, timestamps, base_name):
    """Извлекает скриншоты из видео в указанные моменты времени."""
    screenshots = []
    
    for i, timestamp in enumerate(timestamps, 1):
        screenshot_name = f"{base_name}_screenshot_{i:02d}.png"
        screenshot_path = screenshots_dir / screenshot_name
        
        try:
            # Извлекаем кадр на указанной секунде
            subprocess.run(
                ['ffmpeg', '-y', '-i', str(video_path),
                 '-ss', str(timestamp), '-vframes', '1',
                 '-q:v', '2', str(screenshot_path)],
                capture_output=True,
                check=True
            )
            screenshots.append((timestamp, screenshot_name))
        except subprocess.CalledProcessError as e:
            print(f"Предупреждение: не удалось извлечь скриншот на {timestamp}с: {e}")
            continue
    
    return screenshots


def format_timestamp(seconds):
    """Форматирует секунды в формат MM:SS или HH:MM:SS."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes:02d}:{secs:02d}"


def convert_video_to_md(video_path, model_name='base'):
    """Конвертирует видео файл в Markdown документ."""
    video_path = Path(video_path)
    
    if not video_path.exists():
        print(f"Ошибка: файл {video_path} не найден.")
        return False
    
    print(f"\nОбработка: {video_path.name}")
    print("=" * 60)
    
    # Определяем пути. Вся работа — только в директории с видео (рабочая директория).
    # Никакие файлы не создаются вне этой папки.
    video_dir = video_path.parent
    video_name = video_path.stem
    video_ext = video_path.suffix
    
    # MD файл — в той же папке, где видео
    md_file = video_dir / f"{video_name}.md"
    
    # Папка screenshots — вложенная в папку с видео (рабочую директорию)
    screenshots_dir = video_dir / "screenshots"
    screenshots_dir.mkdir(exist_ok=True)
    
    # Базовое имя для скриншотов (без расширения, с заменой пробелов)
    base_name = video_name.replace(' ', '_').replace('/', '_')
    
    # Проверяем наличие ffmpeg
    has_ffmpeg = check_ffmpeg()
    if not has_ffmpeg:
        print("Предупреждение: ffmpeg не найден. Скриншоты не будут извлечены.")
    
    # Загружаем модель Whisper
    print(f"Загрузка модели Whisper '{model_name}'...")
    try:
        model = whisper.load_model(model_name)
    except Exception as e:
        print(f"Ошибка при загрузке модели: {e}")
        return False
    
    # Распознаем речь
    print("Распознавание речи...")
    try:
        result = model.transcribe(str(video_path), language='ru')
    except Exception as e:
        print(f"Ошибка при распознавании речи: {e}")
        return False
    
    # Получаем сегменты
    segments = result.get('segments', [])
    if not segments:
        print("Предупреждение: не найдено сегментов речи в видео.")
        return False
    
    print(f"Найдено {len(segments)} сегментов речи.")
    
    # Определяем ключевые моменты для скриншотов
    screenshots = []
    if has_ffmpeg and segments:
        # Выбираем до 15 равномерно распределенных моментов
        max_screenshots = min(15, len(segments))
        
        # Получаем длительность видео для равномерного распределения
        duration = get_video_duration(video_path)
        if duration:
            # Выбираем моменты из начала каждого сегмента
            selected_indices = []
            if max_screenshots == 1:
                selected_indices = [0]
            else:
                step = len(segments) / max_screenshots
                selected_indices = [int(i * step) for i in range(max_screenshots)]
            
            timestamps = [segments[i]['start'] for i in selected_indices if i < len(segments)]
        else:
            # Если не удалось получить длительность, берем начало каждого N-го сегмента
            step = max(1, len(segments) // max_screenshots)
            timestamps = [seg['start'] for i, seg in enumerate(segments) if i % step == 0]
            timestamps = timestamps[:max_screenshots]
        
        # Извлекаем скриншоты
        print(f"Извлечение {len(timestamps)} скриншотов...")
        screenshots = extract_screenshots(video_path, screenshots_dir, timestamps, base_name)
        print(f"Извлечено {len(screenshots)} скриншотов.")
    
    # Создаем словарь скриншотов по времени для быстрого поиска
    screenshot_map = {ts: name for ts, name in screenshots}
    
    # Формируем Markdown документ
    print("Создание Markdown документа...")
    
    md_content = f"# {video_name}\n\n"
    md_content += f"*Автоматически создано из видео: {video_path.name}*\n\n"
    md_content += f"*Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
    md_content += "---\n\n"
    
    # Полный текст транскрипции
    full_text = result.get('text', '').strip()
    if full_text:
        md_content += "## Полный текст транскрипции\n\n"
        md_content += f"{full_text}\n\n"
        md_content += "---\n\n"
    
    # Сегменты с временными метками
    md_content += "## Сегменты с временными метками\n\n"
    
    for i, segment in enumerate(segments, 1):
        start = segment.get('start', 0)
        end = segment.get('end', 0)
        text = segment.get('text', '').strip()
        
        timestamp_str = format_timestamp(start)
        md_content += f"### [{timestamp_str}] Сегмент {i}\n\n"
        md_content += f"{text}\n\n"
        
        # Добавляем скриншот, если есть для этого момента
        if start in screenshot_map:
            screenshot_name = screenshot_map[start]
            # Относительный путь от MD файла к папке screenshots (вложена в ту же директорию)
            relative_path = f"screenshots/{screenshot_name}"
            md_content += f"![Скриншот {timestamp_str}]({relative_path})\n\n"
        
        md_content += "---\n\n"
    
    # Сохраняем MD файл
    try:
        md_file.write_text(md_content, encoding='utf-8')
        print(f"✓ Markdown файл создан: {md_file}")
    except Exception as e:
        print(f"Ошибка при сохранении MD файла: {e}")
        return False
    
    # Переименовываем исходное видео
    converted_video = video_dir / f"{video_name}-converted{video_ext}"
    try:
        video_path.rename(converted_video)
        print(f"✓ Видео переименовано: {converted_video.name}")
    except Exception as e:
        print(f"Предупреждение: не удалось переименовать видео: {e}")
    
    print(f"\n✓ Обработка завершена успешно!")
    print(f"  - MD файл: {md_file}")
    print(f"  - Скриншоты: {len(screenshots)} шт. в {screenshots_dir}")
    print(f"  - Всё сохранено в рабочей директории: {video_dir}")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Конвертирует видео файлы в Markdown документы с распознаванием речи и извлечением скриншотов.'
    )
    parser.add_argument(
        'video_files',
        nargs='+',
        help='Путь(и) к видео файлу(ам) для конвертации'
    )
    parser.add_argument(
        '--model',
        default='base',
        choices=['tiny', 'base', 'small', 'medium', 'large'],
        help='Модель Whisper для распознавания (по умолчанию: base)'
    )
    
    args = parser.parse_args()
    
    success_count = 0
    fail_count = 0
    
    for video_file in args.video_files:
        if convert_video_to_md(video_file, args.model):
            success_count += 1
        else:
            fail_count += 1
    
    print("\n" + "=" * 60)
    print(f"Итого: успешно {success_count}, ошибок {fail_count}")
    
    if fail_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
