#!/usr/bin/env python3
"""
Скрипт для конвертации видео файлов в Markdown документы с автоматическим
распознаванием речи и извлечением скриншотов из ключевых моментов.

Автор: DimaTorzok
Версия: 2.0 (офлайн-режим, без интернет-соединений)
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


# ---------------------------------------------------------------------------
# Проверка наличия модели в локальном кэше (офлайн-режим)
# ---------------------------------------------------------------------------

def get_whisper_cache_dir():
    """Определяет путь к директории кэша Whisper."""
    # Whisper использует XDG_CACHE_HOME или ~/.cache/whisper
    cache_home = os.environ.get('XDG_CACHE_HOME')
    if cache_home:
        return Path(cache_home) / 'whisper'
    else:
        return Path.home() / '.cache' / 'whisper'


def check_model_in_cache(model_name):
    """
    Проверяет наличие модели в локальном кэше.
    Возвращает путь к файлу модели, если он существует, иначе None.
    """
    cache_dir = get_whisper_cache_dir()
    
    # Определяем имя файла модели
    model_file = f"{model_name}.pt"
    model_path = cache_dir / model_file
    
    if model_path.exists() and model_path.is_file():
        return model_path
    return None


def load_model_offline(model_name):
    """
    Загружает модель Whisper из локального кэша (офлайн-режим).
    Выдает ошибку, если модель не найдена в кэше.
    """
    # Проверяем наличие модели в кэше
    model_path = check_model_in_cache(model_name)
    
    if model_path is None:
        cache_dir = get_whisper_cache_dir()
        print(f"Ошибка: модель '{model_name}' не найдена в локальном кэше.")
        print(f"Ожидаемый путь: {cache_dir / f'{model_name}.pt'}")
        print("\nДля работы в офлайн-режиме необходимо предварительно загрузить модель.")
        print("Запустите скрипт с интернет-соединением один раз, чтобы загрузить модель в кэш.")
        print(f"Или загрузите модель вручную в директорию: {cache_dir}")
        return None
    
    print(f"Модель '{model_name}' найдена в локальном кэше: {model_path}")
    
    # Загружаем модель из кэша
    # whisper.load_model() автоматически использует кэш, если модель там есть
    # Поскольку мы уже проверили наличие модели, она будет загружена из кэша без сетевых запросов
    try:
        # Whisper автоматически использует кэш из ~/.cache/whisper/ или XDG_CACHE_HOME/whisper/
        # Если модель есть в кэше, сетевых запросов не будет
        model = whisper.load_model(model_name)
        print(f"✓ Модель '{model_name}' загружена из локального кэша (офлайн-режим)")
        return model
    except Exception as e:
        error_msg = str(e).lower()
        # Проверяем, не связана ли ошибка с попыткой загрузки из сети
        if any(keyword in error_msg for keyword in ['connection', 'network', 'download', 'url', 'http']):
            print(f"Ошибка: попытка сетевого соединения при загрузке модели.")
            print(f"Убедитесь, что модель '{model_name}' полностью загружена в кэш.")
            print(f"Ожидаемый путь: {model_path}")
        else:
            print(f"Ошибка при загрузке модели из кэша: {e}")
        return None


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
    if video_path is None:
        return None
    path = Path(video_path) if not isinstance(video_path, Path) else video_path
    if not str(path).strip() or not path.is_file():
        return None
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries',
             'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1',
             str(path)],
            capture_output=True,
            text=True,
            check=True
        )
        raw = (result.stdout or '').strip()
        return float(raw) if raw else None
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError, TypeError):
        return None


def extract_screenshots(video_path, screenshots_dir, timestamps, base_name):
    """Извлекает скриншоты из видео в указанные моменты времени."""
    screenshots = []
    base_name = (base_name or "video").replace(' ', '_').replace('/', '_')

    for i, timestamp in enumerate(timestamps, 1):
        if timestamp is None:
            continue
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
    if seconds is None:
        seconds = 0
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        seconds = 0
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes:02d}:{secs:02d}"


def convert_video_to_md(video_path, model_name='base'):
    """Конвертирует видео файл в Markdown документ."""
    # Защита от пустого или невалидного пути
    if video_path is None:
        print("Ошибка: путь к видео не указан (None).")
        return False
    raw = str(video_path).strip()
    if not raw:
        print("Ошибка: путь к видео пустой или содержит только пробелы.")
        return False

    video_path = Path(video_path)
    model_name = (model_name or '').strip() or 'base'

    if not video_path.exists():
        print(f"Ошибка: файл {video_path} не найден.")
        return False
    if not video_path.is_file():
        print(f"Ошибка: {video_path} не является файлом (возможно, это каталог).")
        return False
    
    print(f"\nОбработка: {video_path.name}")
    print("=" * 60)
    print("Режим работы: ОФЛАЙН (без интернет-соединений)")
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
    base_name = (video_name or "video").replace(' ', '_').replace('/', '_')
    
    # Проверяем наличие ffmpeg
    has_ffmpeg = check_ffmpeg()
    if not has_ffmpeg:
        print("Предупреждение: ffmpeg не найден. Скриншоты не будут извлечены.")

    # Загружаем модель Whisper из локального кэша (офлайн-режим)
    print(f"Загрузка модели Whisper '{model_name}' из локального кэша...")
    model = load_model_offline(model_name)
    if model is None:
        return False

    # Распознаём речь (офлайн-режим, без сетевых запросов)
    print("Распознавание речи...")
    try:
        result = model.transcribe(str(video_path), language='ru')
    except Exception as e:
        print(f"Ошибка при распознавании речи: {e}")
        return False

    # Защита от пустого ответа Whisper
    if result is None:
        print("Ошибка: Whisper вернул пустой результат.")
        return False

    # Получаем сегменты
    segments = (result or {}).get('segments', [])
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
    full_text = (result or {}).get('text', '') or ''
    full_text = full_text.strip() if isinstance(full_text, str) else ''
    if full_text:
        md_content += "## Полный текст транскрипции\n\n"
        md_content += f"{full_text}\n\n"
        md_content += "---\n\n"
    
    # Сегменты с временными метками
    md_content += "## Сегменты с временными метками\n\n"
    
    for i, segment in enumerate(segments, 1):
        start = segment.get('start')
        start = 0 if start is None else start
        end = segment.get('end', 0)
        text = (segment.get('text') or '').strip() if isinstance(segment.get('text'), str) else ''
        
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
        description='Конвертирует видео файлы в Markdown документы с распознаванием речи и извлечением скриншотов. '
                    'Работает в офлайн-режиме (без интернет-соединений). Требует предварительной загрузки модели в кэш.'
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

    # Фильтрация пустых и невалидных путей (защита от "Request is empty" и подобных сбоев)
    video_files = []
    if args.video_files:
        for p in args.video_files:
            if p is not None:
                p_str = str(p).strip()
                if p_str and p_str.lower() not in ('none', 'null', ''):
                    video_files.append(p_str)
    
    if not video_files:
        print("Ошибка: не указано ни одного файла для конвертации.")
        print("Использование: python3 scripts/convert_video_to_md.py <файл1> [файл2 ...] [--model base]")
        print("\nПример:")
        print('  python3 scripts/convert_video_to_md.py "путь/к/видео.mp4"')
        sys.exit(1)
    model = (args.model or '').strip() or 'base'

    success_count = 0
    fail_count = 0

    for video_file in video_files:
        if convert_video_to_md(video_file, model):
            success_count += 1
        else:
            fail_count += 1
    
    print("\n" + "=" * 60)
    print(f"Итого: успешно {success_count}, ошибок {fail_count}")
    
    if fail_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
