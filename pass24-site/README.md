# pass24online — сайт на WordPress

Репозиторий для разработки и поддержки сайта [pass24.online](https://pass24.online) — корпоративного сайта системы управления доступом PASS24.

## Структура проекта

```
├── AGENTS.md                 # Правила работы для агентов (главный файл)
├── claude.md                 # Настройки Claude Code (импортирует AGENTS.md)
├── .env.example              # Шаблон переменных окружения
├── .cursorignore             # Правила видимости файлов для Cursor AI
├── .vscode/settings.json     # Настройки VS Code
└── agent_docs/               # Проектная документация
    ├── index.md              # Навигация по документам
    ├── architecture.md       # Архитектура WordPress-сайта
    ├── adr.md                # Архитектурные решения
    ├── development-history.md # История разработки
    └── guides/               # Гайды (DoD, окружение, логирование)
```

## Быстрый старт

1. Скопируйте `.env.example` в `.env` и заполните данные для подключения
2. Прочитайте `AGENTS.md` — там описаны правила работы над проектом
3. Изучите архитектуру в `agent_docs/architecture.md`

## Документация

- `AGENTS.md` — контекст проекта и правила для ИИ-агентов
- `agent_docs/architecture.md` — архитектура WordPress-сайта
- `agent_docs/index.md` — карта всех документов
