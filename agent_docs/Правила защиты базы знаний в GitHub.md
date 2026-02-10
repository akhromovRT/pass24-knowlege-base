# Правила защиты базы знаний в GitHub

**Версия:** 1.0  
**Дата создания:** 2026-02-09  
**Назначение:** Исчерпывающее руководство по настройке правил защиты базы знаний от неконтролируемых изменений и удаления документов без согласования

---

## Оглавление

1. [Обзор защиты](#обзор-защиты)
2. [Branch Protection Rules](#branch-protection-rules)
3. [GitHub Actions для автоматической проверки](#github-actions-для-автоматической-проверки)
4. [Защита критических файлов и папок](#защита-критических-файлов-и-папок)
5. [Правила для Pull Requests](#правила-для-pull-requests)
6. [Мониторинг и уведомления](#мониторинг-и-уведомления)
7. [Пошаговая настройка](#пошаговая-настройка)

---

## Обзор защиты

### Цели защиты

База знаний компании PASS24.online содержит критически важную информацию, которая должна быть защищена от:

- **Удаления документов** без согласования
- **Массовых изменений** без ревью
- **Изменения структуры** разделов без уведомления
- **Модификации критических файлов** (README, AGENTS.md, шаблоны)
- **Прямых коммитов в main** без прохождения процесса ревью

### Уровни защиты

1. **Уровень 1: Branch Protection** — защита ветки `main` от прямых изменений
2. **Уровень 2: Automated Checks** — автоматические проверки через GitHub Actions
3. **Уровень 3: File Protection** — защита критических файлов через CODEOWNERS
4. **Уровень 4: PR Requirements** — обязательные требования к Pull Requests

---

## Branch Protection Rules

### Настройка защиты ветки `main`

**Путь:** `Settings` → `Branches` → `Add rule` → выбрать ветку `main`

#### Обязательные настройки:

1. **Require a pull request before merging**
   - ✅ Включено
   - **Required number of approvals:** `1` (требуется ваше одобрение)
   - **Dismiss stale pull request approvals when new commits are pushed:** ✅ Включено
   - **Require review from Code Owners:** ✅ Включено

2. **Require status checks to pass before merging**
   - ✅ Включено
   - **Required status checks:**
     - `check-file-deletions` (проверка удалений)
     - `check-critical-files` (проверка критических файлов)
     - `validate-markdown` (валидация Markdown)
   - **Require branches to be up to date before merging:** ✅ Включено

3. **Require conversation resolution before merging**
   - ✅ Включено

4. **Do not allow bypassing the above settings**
   - ✅ Включено (даже для администраторов)

5. **Restrict who can push to matching branches**
   - ✅ Включено
   - Разрешить push только владельцу репозитория

6. **Allow force pushes**
   - ❌ Отключено

7. **Allow deletions**
   - ❌ Отключено

8. **Require linear history**
   - ✅ Включено (рекомендуется)

---

## GitHub Actions для автоматической проверки

### Структура workflows

Создать папку `.github/workflows/` и добавить следующие файлы:

```
.github/
└── workflows/
    ├── check-file-deletions.yml
    ├── check-critical-files.yml
    ├── validate-markdown.yml
    └── require-pr-description.yml
```

### 1. Проверка удаления файлов (`check-file-deletions.yml`)

**Назначение:** Блокирует PR, если удаляются файлы из базы знаний без явного указания в описании PR.

```yaml
name: Check File Deletions

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  check-deletions:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Detect deleted files
        id: detect-deletions
        run: |
          DELETED_FILES=$(git diff --name-only --diff-filter=D origin/main...HEAD | grep -E '\.(md|mdx)$' || true)
          
          if [ -z "$DELETED_FILES" ]; then
            echo "deleted=false" >> $GITHUB_OUTPUT
            echo "files=" >> $GITHUB_OUTPUT
          else
            echo "deleted=true" >> $GITHUB_OUTPUT
            echo "files<<EOF" >> $GITHUB_OUTPUT
            echo "$DELETED_FILES" >> $GITHUB_OUTPUT
            echo "EOF" >> $GITHUB_OUTPUT
          fi

      - name: Check PR description for deletion notice
        if: steps.detect-deletions.outputs.deleted == 'true'
        run: |
          PR_BODY="${{ github.event.pull_request.body }}"
          DELETED_FILES="${{ steps.detect-deletions.outputs.files }}"
          
          # Проверяем наличие ключевых слов в описании PR
          if echo "$PR_BODY" | grep -qiE "(удал|delete|remove|DELETION|УДАЛЕНИЕ)"; then
            echo "✅ Удаление документов явно указано в описании PR"
            exit 0
          else
            echo "❌ ОШИБКА: Обнаружены удалённые файлы, но это не указано в описании PR!"
            echo ""
            echo "Удалённые файлы:"
            echo "$DELETED_FILES"
            echo ""
            echo "Пожалуйста, добавьте в описание PR явное упоминание об удалении файлов:"
            echo "- Укажите причину удаления"
            echo "- Перечислите удаляемые файлы"
            echo "- Добавьте метку [DELETION] в название или описание PR"
            exit 1
          fi

      - name: Fail on critical file deletions
        if: steps.detect-deletions.outputs.deleted == 'true'
        run: |
          DELETED_FILES="${{ steps.detect-deletions.outputs.files }}"
          
          # Критические файлы, которые нельзя удалять
          CRITICAL_FILES=(
            "AGENTS.md"
            "agent_docs/Инструкция по работе с базой знаний.md"
            "agent_docs/Шаблон статьи базы знаний.md"
            "База знаний/README.md"
          )
          
          for critical in "${CRITICAL_FILES[@]}"; do
            if echo "$DELETED_FILES" | grep -q "$critical"; then
              echo "❌ КРИТИЧЕСКАЯ ОШИБКА: Попытка удалить критический файл: $critical"
              echo "Удаление этого файла требует прямого согласования с владельцем репозитория."
              exit 1
            fi
          done
```

### 2. Проверка критических файлов (`check-critical-files.yml`)

**Назначение:** Требует обязательного ревью при изменении критических файлов.

```yaml
name: Check Critical Files

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  check-critical:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Detect critical file changes
        run: |
          CRITICAL_FILES=(
            "AGENTS.md"
            "agent_docs/Инструкция по работе с базой знаний.md"
            "agent_docs/Шаблон статьи базы знаний.md"
            "agent_docs/adr.md"
            "agent_docs/architecture.md"
            ".github/workflows/*.yml"
            ".github/CODEOWNERS"
          )
          
          CHANGED_FILES=$(git diff --name-only origin/main...HEAD)
          
          CRITICAL_CHANGED=false
          for critical in "${CRITICAL_FILES[@]}"; do
            if echo "$CHANGED_FILES" | grep -q "$critical"; then
              echo "⚠️  Изменён критический файл: $critical"
              CRITICAL_CHANGED=true
            fi
          done
          
          if [ "$CRITICAL_CHANGED" = true ]; then
            echo "critical_changed=true" >> $GITHUB_ENV
            echo "⚠️  ВНИМАНИЕ: Изменены критические файлы. Требуется обязательное ревью владельца."
          else
            echo "critical_changed=false" >> $GITHUB_ENV
          fi

      - name: Require owner review for critical files
        if: env.critical_changed == 'true'
        run: |
          PR_AUTHOR="${{ github.event.pull_request.user.login }}"
          REPO_OWNER="${{ github.repository_owner }}"
          
          if [ "$PR_AUTHOR" != "$REPO_OWNER" ]; then
            echo "⚠️  Изменения критических файлов требуют ревью от $REPO_OWNER"
            echo "Пожалуйста, дождитесь одобрения перед мерджем."
          fi
```

### 3. Валидация Markdown (`validate-markdown.yml`)

**Назначение:** Проверяет корректность Markdown файлов.

```yaml
name: Validate Markdown

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  validate-markdown:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install markdownlint-cli
        run: npm install -g markdownlint-cli

      - name: Lint Markdown files
        run: |
          # Проверяем только изменённые файлы
          CHANGED_FILES=$(git diff --name-only --diff-filter=ACMR origin/main...HEAD | grep -E '\.(md|mdx)$' || true)
          
          if [ -z "$CHANGED_FILES" ]; then
            echo "Нет изменённых Markdown файлов для проверки"
            exit 0
          fi
          
          echo "$CHANGED_FILES" | xargs markdownlint --config .markdownlint.json || true
```

### 4. Требование описания PR (`require-pr-description.yml`)

**Назначение:** Блокирует PR без описания или с недостаточным описанием.

```yaml
name: Require PR Description

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  check-description:
    runs-on: ubuntu-latest
    steps:
      - name: Check PR description
        run: |
          PR_BODY="${{ github.event.pull_request.body }}"
          PR_TITLE="${{ github.event.pull_request.title }}"
          
          # Минимальная длина описания (без пробелов)
          MIN_LENGTH=50
          
          if [ -z "$PR_BODY" ] || [ ${#PR_BODY} -lt $MIN_LENGTH ]; then
            echo "❌ ОШИБКА: PR должен содержать описание минимум $MIN_LENGTH символов"
            echo ""
            echo "Текущее описание:"
            echo "$PR_BODY"
            echo ""
            echo "Пожалуйста, добавьте описание, включающее:"
            echo "- Что изменено"
            echo "- Зачем это нужно"
            echo "- Какие файлы затронуты"
            exit 1
          fi
          
          # Проверка на наличие ключевых слов при удалении файлов
          DELETED_FILES=$(git diff --name-only --diff-filter=D origin/main...HEAD 2>/dev/null | grep -E '\.(md|mdx)$' || true)
          
          if [ -n "$DELETED_FILES" ]; then
            if ! echo "$PR_BODY" | grep -qiE "(удал|delete|remove|DELETION|УДАЛЕНИЕ)"; then
              echo "⚠️  ВНИМАНИЕ: Обнаружены удалённые файлы, но это не указано в описании PR"
              echo "Удалённые файлы:"
              echo "$DELETED_FILES"
              exit 1
            fi
          fi
          
          echo "✅ Описание PR соответствует требованиям"
```

---

## Защита критических файлов и папок

### CODEOWNERS файл

Создать файл `.github/CODEOWNERS` для автоматического назначения ревьюеров:

```
# Критические файлы проекта - требуют обязательного ревью владельца
/AGENTS.md @akhromovRT
/agent_docs/Инструкция по работе с базой знаний.md @akhromovRT
/agent_docs/Шаблон статьи базы знаний.md @akhromovRT
/agent_docs/adr.md @akhromovRT
/agent_docs/architecture.md @akhromovRT

# Все файлы в agent_docs требуют ревью
/agent_docs/ @akhromovRT

# README файлы в основных разделах
/База знаний/README.md @akhromovRT
/База знаний/*/README.md @akhromovRT

# GitHub конфигурация
/.github/ @akhromovRT

# Удаление любых файлов из базы знаний требует ревью
/База знаний/**/*.md @akhromovRT
```

**Настройка:** `Settings` → `Code owners` → включить "Require review from Code Owners"

### Защита от массовых изменений

Добавить проверку в GitHub Action для обнаружения массовых изменений:

```yaml
- name: Detect bulk changes
  run: |
    CHANGED_FILES=$(git diff --name-only --diff-filter=ACMR origin/main...HEAD | wc -l)
    
    # Порог массовых изменений (настраивается)
    BULK_THRESHOLD=20
    
    if [ "$CHANGED_FILES" -gt "$BULK_THRESHOLD" ]; then
      echo "⚠️  ВНИМАНИЕ: Обнаружено массовое изменение ($CHANGED_FILES файлов)"
      echo "Массовые изменения требуют особого внимания и детального описания в PR."
      echo ""
      echo "Пожалуйста, убедитесь, что:"
      echo "1. Все изменения описаны в PR"
      echo "2. Изменения согласованы"
      echo "3. Нет случайных изменений"
    fi
```

---

## Правила для Pull Requests

### Обязательные требования к PR

1. **Заголовок PR:**
   - Должен быть информативным
   - Использовать префиксы: `docs:`, `feat:`, `fix:`, `refactor:`
   - Пример: `docs: добавлена инструкция по работе с CRM`

2. **Описание PR (минимум 50 символов):**
   - Что изменено
   - Зачем это нужно
   - Какие файлы затронуты
   - Если удаляются файлы — явно указать и обосновать

3. **Метки для категоризации:**
   - `[DELETION]` — при удалении файлов
   - `[CRITICAL]` — при изменении критических файлов
   - `[BULK]` — при массовых изменениях (>20 файлов)

4. **Чек-лист в описании PR:**
   ```markdown
   - [ ] Изменения соответствуют стандартам базы знаний
   - [ ] Обновлены README файлы (если требуется)
   - [ ] Документы соответствуют шаблону
   - [ ] Удаление файлов явно указано и обосновано (если применимо)
   ```

### Шаблон Pull Request

Создать файл `.github/pull_request_template.md`:

```markdown
## Описание изменений

<!-- Опишите, что изменено и зачем это нужно -->

## Затронутые файлы

<!-- Перечислите основные файлы, которые были изменены -->

- [ ] Файл 1
- [ ] Файл 2

## Тип изменений

- [ ] Добавление нового документа
- [ ] Обновление существующего документа
- [ ] Удаление документа (⚠️ требует обоснования)
- [ ] Изменение структуры раздела
- [ ] Обновление README
- [ ] Изменение критических файлов (AGENTS.md, шаблоны)

## Если удаляются файлы

<!-- Заполните, если удаляются какие-либо файлы -->

**Удаляемые файлы:**
- 

**Причина удаления:**
- 

**Согласовано с:** @akhromovRT

## Чек-лист

- [ ] Изменения соответствуют стандартам базы знаний
- [ ] Обновлены README файлы в затронутых разделах
- [ ] Документы соответствуют шаблону статьи
- [ ] Удаление файлов явно указано и обосновано (если применимо)
- [ ] Изменения критических файлов согласованы (если применимо)

## Дополнительная информация

<!-- Любая дополнительная информация, которая может быть полезна при ревью -->
```

---

## Мониторинг и уведомления

### Настройка уведомлений

1. **GitHub Notifications:**
   - `Settings` → `Notifications` → включить уведомления о PR
   - Настроить email-уведомления для критических событий

2. **Webhook для внешних уведомлений (опционально):**
   - Можно настроить webhook для отправки уведомлений в Slack/Telegram при:
     - Создании PR с удалением файлов
     - Изменении критических файлов
     - Массовых изменениях

### GitHub Actions для уведомлений

```yaml
name: Notify on Critical Changes

on:
  pull_request:
    types: [opened]

jobs:
  notify:
    runs-on: ubuntu-latest
    steps:
      - name: Check for critical changes
        run: |
          # Проверка удалений
          DELETED=$(git diff --name-only --diff-filter=D origin/main...HEAD | grep -E '\.md$' | wc -l)
          
          if [ "$DELETED" -gt 0 ]; then
            echo "⚠️  Обнаружено удаление $DELETED файлов"
            # Здесь можно добавить отправку уведомления
          fi
```

---

## Пошаговая настройка

### Шаг 1: Создать структуру GitHub Actions

```bash
mkdir -p .github/workflows
mkdir -p .github
```

### Шаг 2: Создать файлы workflows

Создать все 4 файла из раздела [GitHub Actions](#github-actions-для-автоматической-проверки):
- `check-file-deletions.yml`
- `check-critical-files.yml`
- `validate-markdown.yml`
- `require-pr-description.yml`

### Шаг 3: Создать CODEOWNERS

Создать файл `.github/CODEOWNERS` с правилами из раздела [CODEOWNERS](#codeowners-файл)

### Шаг 4: Создать шаблон PR

Создать файл `.github/pull_request_template.md` с шаблоном из раздела [Шаблон Pull Request](#шаблон-pull-request)

### Шаг 5: Настроить Branch Protection

1. Перейти в `Settings` → `Branches`
2. Нажать `Add rule`
3. Выбрать ветку `main`
4. Применить все настройки из раздела [Branch Protection Rules](#branch-protection-rules)

### Шаг 6: Включить CODEOWNERS

1. Перейти в `Settings` → `Code owners`
2. Включить "Require review from Code Owners"

### Шаг 7: Протестировать защиту

1. Создать тестовую ветку
2. Попытаться удалить файл
3. Создать PR без описания
4. Проверить, что все проверки срабатывают

---

## Дополнительные рекомендации

### Для владельца репозитория

1. **Регулярно проверять PR:** Настроить ежедневные уведомления о новых PR
2. **Использовать метки:** Создать метки `[DELETION]`, `[CRITICAL]`, `[BULK]` для быстрой категоризации
3. **Вести журнал изменений:** Отслеживать все удаления и массовые изменения в `agent_docs/development-history.md`

### Для контрибьюторов

1. **Всегда создавать ветку** перед изменениями
2. **Проверять `git status`** перед коммитом
3. **Писать подробные описания PR** при удалении файлов
4. **Обновлять README** при изменении структуры разделов

### Резервное копирование

Рекомендуется настроить автоматическое резервное копирование:

1. **GitHub автоматически** создаёт резервные копии через Git
2. **Дополнительно:** Можно настроить автоматический экспорт в другой репозиторий или облачное хранилище

---

## Итоговый чек-лист настройки

- [ ] Создана папка `.github/workflows/` с 4 файлами проверок
- [ ] Создан файл `.github/CODEOWNERS`
- [ ] Создан файл `.github/pull_request_template.md`
- [ ] Настроены Branch Protection Rules для `main`
- [ ] Включено требование ревью от Code Owners
- [ ] Настроены уведомления о PR
- [ ] Протестирована защита на тестовом PR
- [ ] Созданы метки для PR: `[DELETION]`, `[CRITICAL]`, `[BULK]`

---

## Поддержка и обновление

При необходимости обновления правил защиты:

1. Изменить соответствующие файлы в `.github/`
2. Протестировать изменения на тестовом PR
3. Обновить версию этого документа
4. Зафиксировать изменения в `agent_docs/development-history.md`

---

**Версия документа:** 1.0  
**Последнее обновление:** 2026-02-09  
**Ответственный:** @akhromovRT
