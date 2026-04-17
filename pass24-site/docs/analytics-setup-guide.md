# Аналитика — Инструкция по настройке

> Ручные шаги в admin-панелях после деплоя кода.

---

## 1. GTM (tagmanager.google.com)

Container: **GTM-N5S4CR6T**

### 1.1 GA4 Configuration Tag

1. Variables → New → Type: **Constant** → Name: `GA4 Measurement ID` → Value: `G-XXXXXXXXX` (подставить реальный)
2. Tags → New → Type: **Google Analytics: GA4 Configuration**
   - Measurement ID: `{{GA4 Measurement ID}}`
   - Trigger: **All Pages**

### 1.2 Event Tags (для каждого события)

Создать Tag + Trigger для каждого:

| Event name | Trigger type | GA4 Tag type |
|------------|-------------|--------------|
| `demo_request` | Custom Event: `demo_request` | GA4 Event |
| `generate_lead` | Custom Event: `generate_lead` | GA4 Event |
| `demo_form_step` | Custom Event: `demo_form_step` | GA4 Event |
| `contact_form_submit` | Custom Event: `contact_form_submit` | GA4 Event |
| `pricing_viewed` | Custom Event: `pricing_viewed` | GA4 Event |

Для каждого Tag:
- Type: **Google Analytics: GA4 Event**
- Configuration Tag: выбрать GA4 Configuration из п.1.1
- Event Name: имя из таблицы
- Event Parameters (опционально): `event_category`, `form_name`, `object_type` — из dataLayer

### 1.3 Publish

Submit → Publish container.

---

## 2. GA4 (analytics.google.com)

1. Admin → Property → Events
2. Найти: `generate_lead`, `demo_request`, `contact_form_submit`
3. Включить переключатель **Mark as conversion** для каждого

---

## 3. Яндекс.Метрика (metrika.yandex.ru)

Счётчик: **106989472**

### 3.1 Цели

Настройки → Цели → Добавить цель:

| Название | Тип | Идентификатор |
|----------|-----|---------------|
| Демо-заявка | JavaScript-событие | `demo_request` |
| Демо шаг 1 | JavaScript-событие | `demo_step_1` |
| Демо шаг 2 | JavaScript-событие | `demo_step_2` |
| Демо шаг 3 | JavaScript-событие | `demo_step_3` |
| Генерация лида | JavaScript-событие | `generate_lead` |
| Контактная форма | JavaScript-событие | `contact_form_submit` |
| Просмотр тарифов | JavaScript-событие | `pricing_viewed` |

### 3.2 Вебвизор

Настройки → Вебвизор, карта скроллов, аналитика форм → **Включить все** → Сохранить.

---

## 4. Rank Math (WP Admin)

### 4.1 Google Search Console

Rank Math → Dashboard → Analytics → **Connect Google Services** → авторизовать Google-аккаунт → выбрать property `pass24pro.ru`.

### 4.2 Local SEO / Schema

Rank Math → Titles & Meta → Local SEO:
- **Company Name:** PASS24.online
- **Phone:** +7 (495) 414-44-55
- **Email:** info@pass24.online
- **Address:** Москва, Россия
- **Business Type:** Organization

> JSON-LD Schema (Organization + SoftwareApplication) уже выводится через `functions.php` → `pass24_schema_jsonld()`. Rank Math Local SEO — дополнительный источник для поисковиков.

---

## 5. Верификация

После настройки:

1. **GTM Preview:** tagmanager.google.com → Preview → ввести `https://pass24pro.ru` → проверить что GA4 Configuration Tag и event tags стреляют
2. **GA4 Realtime:** analytics.google.com → Reports → Realtime → зайти на сайт → убедиться что events видны
3. **Метрика:** metrika.yandex.ru → Целевые визиты → подождать 24 часа, затем проверить что цели регистрируются
4. **Schema:** https://search.google.com/test/rich-results → ввести `https://pass24pro.ru` → убедиться что Organization и SoftwareApplication распознаны
