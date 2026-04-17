# История разработки

Правило: хранить только последние 10 записей. При добавлении новой переносить старые в
`agent_docs/development-history-archive.md`. Архив читать при необходимости.

---

Краткий журнал итераций проекта.

## Записи

### 2026-03-02 — Фаза 4: кейсы (5 страниц) + блог (10 статей)

**Кейсы (/cases/):**
- `inc/case-data.php` — данные 5 кейсов (Садовые Кварталы, Агаларов, MLP, Амфион, Нагатино)
- `template-parts/case-page.php` — shared шаблон 7 секций (hero, задача, решение, метрики, отзыв, продукты, CTA)
- `page-cases.php` — hub с карточной сеткой 2 колонки
- 5 thin dispatchers: `page-sadovye-kvartaly.php`, `page-agalarov.php`, `page-mlp.php`, `page-amphion.php`, `page-nagatino.php`
- `assets/css/cases.css` — стили кейсов (метрики, testimonial, hub-карточки)

**Блог (/blog/):**
- `archive.php` — список постов с фильтрацией по категориям, пагинация, CTA
- `home.php` — делегирует в archive.php (WordPress Posts page)
- `single.php` — шаблон статьи: хлебные крошки, мета (дата, время чтения), контент, inline CTA, related posts, навигация
- `assets/css/blog.css` — стили: карточки постов, типографика статьи, пагинация, breadcrumbs
- 9 категорий (cloud-skud, zhk, cottage-village, skud-bc, mobile-passes, lpr-technology, reviews, guides, logistics-production)
- 10 SEO-статей с полным контентом (800-1200 слов каждая)

**Общее:**
- `functions.php` — conditional enqueue для cases.css и blog.css
- `design-system.css` — full-width overrides для cases и blog body classes
- Домен: pass24pro.ru (WordPress siteurl)

**Commit:** `820200c` feat: Phase 4 — кейсы (5 страниц + hub) и блог (archive, single, home)

### 2026-03-02 — Фаза 3 (3.1–3.3): продукты, решения, интеграции

Реализована data-driven архитектура шаблонов для 17 новых страниц.

**Архитектура:**
- `inc/product-data.php` — массив 6 продуктов, `inc/solution-data.php` — массив 8 решений, `inc/integration-data.php` — 7 партнёров
- `template-parts/product-page.php` (7 секций), `template-parts/solution-page.php` (8 секций) — shared шаблоны
- 14 thin dispatcher файлов `page-{slug}.php` (~5 строк каждый)
- `page-products.php`, `page-solutions.php` — hub-страницы (карточные сетки)
- `page-integrations.php` — standalone страница интеграций (сетка, Open API, CTA)

**Инфраструктура:**
- `functions.php`: хелпер `pass24_is_child_of_page()`, parent body class `page-parent-{slug}`, conditional enqueue products/solutions/integrations
- `design-system.css`: parent-based full-width selectors (`page-parent-products`, `page-parent-solutions`)
- `solutions.js`: auto-select сегмента в CTA-форме, AJAX-отправка

**CSS:** `products.css`, `solutions.css`, `integrations.css` — shared стили для каждого типа страниц

**Placeholders:** hero-images для продуктов/решений, testimonial authors `[Имя Фамилия]`, construction/security без реальных кейсов

### 2026-03-02 — Фаза 2 завершена: все основные страницы

Полностью реализована Фаза 2 (основные страницы). Все шаблоны — кастомный PHP (без Bricks Builder).

**2.1 Глобальные элементы:**
- `header.php` + `header.js` — шапка с навигацией, мобильное меню, sticky shadow
- `footer.php` — подвал с навигацией, контактами, соцсетями, юр. информацией
- Sticky CTA-бар перенесён из `front-page.php` в `footer.php` (глобально)
- SVG-ассеты перемещены из `assets-temp/` в `assets/img/` (logos, integrations, icons)

**2.2 Главная (12 секций):** `front-page.php` — hero, доверие, проблемы, решение, роли, сегменты, сравнение, кейсы, отзывы, интеграции, награды, финальный CTA

**2.3 Тарифы:** `page-pricing.php` + `pricing.js` + `pricing.css` — 4 тарифа, переключатель год/месяц, FAQ-аккордеон

**2.4 Демо:** `page-demo.php` + `demo.js` + `demo.css` — 3-шаговая форма, auto-save Step 1, REST endpoint `/pass24/v1/demo-request`, fallback на Gravity Forms

**2.5 О компании:** `page-about.php` + `about.css` — статистика, продукты, награды, доверие, клиенты, карта-placeholder

**2.6 Контакты:** `page-contacts.php` + `contacts.js` + `contacts.css` — инфо + форма, REST endpoint `/pass24/v1/contact`, fallback на Gravity Forms (form ID=2)

**REST API:** два endpoint'а в `functions.php` — `demo-request` и `contact` — с отправкой в Bitrix24 webhook + email fallback.

**Placeholders (требуют замены):** телефоны `XXX-XX-XX` (5+ мест), авторы отзывов, hero-image, product images, карта на странице контактов.

### 2026-02-25 — Синхронизация всех документов с blueprint v2.0

После полной перестройки blueprint обновлены все зависимые документы:

- `content-strategy.md` — верифицированные клиенты по сегментам, 8 SEO-кластеров (добавлен кластер логистики/производства), обновлена CTA-матрица, добавлены секции «Верифицированные факты» и «НЕ использовать»
- `architecture.md` — расширена карта сайта (8 вертикалей, разделены /about/ и /partners/), обновлён roadmap, добавлены перекрёстные ссылки на technical-spec.md
- `integrations.md` — добавлены триггеры чат-бота для новых вертикалей (7 URL), промышленная воронка, многошаговая форма захвата лидов с автосохранением
- `implementation-plan.md` — добавлена фаза 1.7 (подготовка контента), реальные цены в тарифах, реальные клиенты в кейсах и вертикалях, A/B-тестирование перенесено в фазу 3, лид-магниты в фазу 4
- `adr.md` — добавлены 6 новых ADR (006–011): разделение документации, прозрачные тарифы, Hero 1+2 A/B, 8 вертикалей, политика верификации фактов, пилот 14 дней как центральный оффер
- `index.md` — обновлены описания adr.md (11 решений) и integrations.md (автоматизация продаж)
- `AGENTS.md` — добавлены P3-сегменты, центральный оффер, ссылка на v2.0 документацию, политика верифицированных фактов

### 2026-02-25 — Blueprint v2.0: полная перестройка

- Разделён monolithic `blueprint.md` на два документа:
  - `agent_docs/blueprint.md` — маркетинговый blueprint (позиционирование, контент, CTA, SEO)
  - `agent_docs/technical-spec.md` — техническое ТЗ (стек, плагины, Битрикс24, аналитика)
- Верифицированы все факты по реальным данным:
  - Клиентская база: 87 верифицированных объектов, 300+ всего
  - Цифра заявок: 1,5 млн/мес (не в год, как было раньше)
  - Тарифы: Lite 10 990, Standard 14 990, Prof 24 990, Business 48 990 ₽/мес
  - Награды: Новатор Москвы 2022, ComNews Awards 2020, Безопасность 2023
- Убраны ложные данные:
  - Логотипы Villagio, ПИК, Самолет, PSN Group, CEO ROOMS → заменены на МТС, MLP, Sezar Group, Садовые Кварталы, Агаларов Эстейт, Росинка
  - Убрана «Группа ЛАНИТ»
  - Убраны неподтверждённые награды EMERGE 2023, PROESTATE 2022
- Добавлена премия «Новатор Москвы» 2022 (Правительство Москвы, Собянин) — ранее отсутствовала
- Hero: один сильный вариант + 2 для A/B вместо 6 слабых
- Расширены вертикали: добавлены логистика, производство, индустриальные парки (по реальной клиентской базе)
- 14-дневный пилот — центральный оффер (включает обучение и сопровождение)
- Убраны все упоминания иностранных брендов-конкурентов, стратегии абстрагированы
- Обновлён `agent_docs/index.md`

### 2026-02-24 — Фаза 2: домен, SSL, аналитика, Bitrix24

- Домен: pass24pro.ru → 194.87.140.245 (DNS подтверждён)
- SSL: Let's Encrypt сертификат (certbot), автообновление настроено, HSTS включён
- WP URL переключён на https://pass24pro.ru, все ссылки в БД заменены
- GTM-N5S4CR6T подключён через плагин GTM4WP (проверено: код в HTML)
- Яндекс.Метрика 106989472 — прямой код в head через functions.php
- Bitrix24: mu-plugin `pass24-bitrix24.php` — CF7 → crm.lead.add (тест: лид ID 2)
- UTM-параметры сохраняются в куки 30 дней, передаются в CRM
- REST API endpoint: POST /wp-json/pass24/v1/lead (для калькулятора ROI)
- CF7 события → dataLayer (GTM) + Яндекс.Метрика цель demo_request

### 2026-02-23 — Интеграция blueprint и подготовка плана реализации

- Изучён и интегрирован `PASS24_Website_Blueprint_RU.md` (ТЗ на сайт)
- Blueprint перемещён в `agent_docs/blueprint.md` как исходный источник истины по текстам
- Полностью обновлён `agent_docs/architecture.md`: карта сайта, техстек, дизайн-система
- Создан `agent_docs/implementation-plan.md` — детальный чек-лист по 6 фазам (Фазы 1–6)
- Создан `agent_docs/content-strategy.md` — SEO-кластеры, контент-план блога, формулы CTA
- Создан `agent_docs/integrations.md` — AI Sales Factory, скоринг лидов, n8n автопубликация
- Обновлён `agent_docs/adr.md` — 5 архитектурных решений (Bricks Builder, Gravity Forms, Rank Math, n8n, скоринг)
- Обновлён `agent_docs/index.md` — навигация по всем новым документам
- Обновлён `AGENTS.md` — расширенное описание проекта

### 2026-02-23 — Инициализация проекта

- Создан репозиторий на основе шаблона 2030ai/project_template
- Настроено окружение: `.vscode/settings.json`, `.cursorignore`, `.env.example`
