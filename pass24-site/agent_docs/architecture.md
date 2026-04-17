# Архитектура

> Синхронизировано с `agent_docs/blueprint.md` v2.0 и `agent_docs/technical-spec.md` (февраль 2026)

## Обзор

Публичный сайт pass24.online на WordPress — входная точка продаж PASS24. Цель: конвертировать органический и платный трафик в квалифицированные лиды с автоматической передачей в Битрикс24.

## Контекст

- **Домен:** pass24pro.ru (WordPress siteurl/home; pass24.online перенаправляет на pass24online.ru — старый сайт)
- **Сервер:** 194.87.140.245 (VPS, root SSH)
- **Платформа:** WordPress 6.5+ (self-hosted)
- **SSL:** Let's Encrypt (certbot, автообновление, HSTS)
- **Аналитика:** GTM-N5S4CR6T + Яндекс.Метрика 106989472

## Карта сайта (полная структура URL)

```
/                                     — Главная (12 секций)
/products/
  /products/mobile-pass-bureau/       — Мобильное бюро пропусков
  /products/pass24-auto/              — PASS24.auto (распознавание номеров)
  /products/pass24-key/               — PASS24.Key (цифровой ключ BLE/NFC)
  /products/pass24-cloud/             — PASS24.Cloud (облачная платформа)
  /products/pass24-parking/           — PASS24.Parking (квотирование)
  /products/cloud-skud/               — Облачный СКУД
/solutions/
  /solutions/residential-complex/     — Для жилых комплексов (P1)
  /solutions/cottage-village/         — Для коттеджных посёлков (P2)
  /solutions/business-center/         — Для бизнес-центров (P2)
  /solutions/logistics/               — Для логистических и складских комплексов (P3)
  /solutions/production/              — Для производственных предприятий (P3)
  /solutions/industrial-park/         — Для индустриальных парков (P3)
  /solutions/construction/            — Для строительных компаний (P3)
  /solutions/security-company/        — Для ЧОП (P3)
/integrations/                        — Интеграции + Open API
/pricing/                             — Тарифы (4 плана с реальными ценами)
/cases/                               — Кейсы (hub, карточная сетка)
  /cases/sadovye-kvartaly/             — Кейс: Садовые Кварталы (ЖК)
  /cases/agalarov/                     — Кейс: Агаларов Эстейт (КП)
  /cases/mlp/                          — Кейс: MLP (логистика)
  /cases/amphion/                      — Кейс: Live Арена Амфион
  /cases/nagatino/                     — Кейс: Нагатино i-Land (БЦ)
/resources/
  /resources/roi-calculator/          — Калькулятор ROI (интерактивный)
  /resources/cloud-vs-local/          — Сравнение облако vs локальная
  /resources/guides/                  — Руководства и вайтпейперы
  /resources/videos/                  — Видео и вебинары
/blog/                                — «Просто о СКУД» (SEO-блог)
/partners/
  /partners/program/                  — Партнёрская программа
  /partners/catalog/                  — Каталог интеграторов
/about/
  /about/awards/                      — Награды и сертификаты
  /about/contacts/                    — Контакты
/demo/                                — Лендинг запроса демо
/security/                            — Безопасность и соответствие
```

## Технологический стек

### Тема и конструктор
| Компонент | Выбор | Причина |
|---|---|---|
| Конструктор страниц | **Bricks Builder** ($79, пожизненная) | Чистейший код, лучшие Core Web Vitals |
| Базовая тема | GeneratePress | Лёгкая, совместима с Bricks |
| Дочерняя тема | child theme | Все кастомизации только здесь |

### Обязательные плагины (не более 20 активных)

**Производительность:**
- WP Rocket — кэш, минификация, оптимизация БД
- ShortPixel — конвертация в WebP/AVIF
- Asset CleanUp — удаление лишних скриптов на страницах
- Perfmatters — удаление лишнего WordPress-кода

**SEO:**
- Rank Math Pro — Schema разметка, AI-функции, focus keywords
- Schema Pro — SoftwareApplication и FAQ Schema

**Формы и CRM:**
- Gravity Forms ($59/год) + **Flamix Bitrix24** (нативная интеграция)
- Многошаговые формы, условная логика, автозахват UTM-параметров

**Аналитика:**
- Google Site Kit (GA4)
- Яндекс.Метрика плагин (Вебвизор, тепловые карты, аналитика форм)
- Google Tag Manager

**Конверсия (CRO):**
- If-So Dynamic Content — персонализация по сегментам/гео
- Nelio A/B Testing — сплит-тест Hero-секций
- OptiMonk — exit-intent попапы и триггеры

**Чат:**
- Битрикс24 Open Lines — встроенный чат с CRM, WhatsApp/VK/Telegram каналы

**Безопасность:**
- Wordfence — файрвол + сканер
- UpdraftPlus — автобэкапы (ежедневно → S3/Yandex Object Storage)
- WP Mail SMTP

Детальная спецификация: `agent_docs/technical-spec.md`

## Потоки данных

```
Посетитель → Cloudflare CDN → nginx → PHP 8.x (WordPress)
                                              │
                                    Bricks Builder + Плагины
                                              │
                              ┌───────────────┴───────────────┐
                              │                               │
                         MySQL БД                    WordPress REST API
                       (контент, настройки)                  │
                                                    n8n автопубликация
                                                    (Claude API → /wp/v2/posts)

Формы Gravity → Flamix webhook → Битрикс24 CRM
Битрикс24 Open Lines чат ─────────────────────┘
Калькулятор ROI → кастомный JS webhook ────────┘
```

## Дизайн-система (токены)

```css
/* Основные цвета */
--primary:        #2563EB;  /* Синий — доверие */
--primary-dark:   #1D4ED8;
--primary-light:  #DBEAFE;
--accent:         #10B981;  /* Зелёный — успех */
--text-primary:   #111827;
--text-secondary: #6B7280;
--bg-primary:     #FFFFFF;
--bg-secondary:   #F9FAFB;
--bg-dark:        #111827;  /* CTA-секции */
--border:         #E5E7EB;

/* Типографика — Inter */
/* H1 Hero:  48px / 800 / 1.1  */
/* H2:       36px / 700 / 1.2  */
/* H3:       24px / 600 / 1.3  */
/* Body:     18px / 400 / 1.6  */
```

## Нефункциональные требования

- LCP < 2.5 сек, INP < 200 мс, CLS < 0.1
- PageSpeed Insights ≥ 90/100 (mobile + desktop)
- Все изображения: WebP + lazy loading ниже первого экрана
- Адаптивный дизайн (mobile-first)
- Активных плагинов ≤ 20
- Архитектура поддерживает мультиязычность (WPML) для будущего выхода на MENA

## Roadmap по фазам

| Фаза | Сроки | Задачи | Статус |
|---|---|---|---|
| 1 — Фундамент | Нед. 1–2 | WordPress + стек + GA4 + Метрика + Битрикс24 | **Done** |
| 2 — Страницы | Нед. 2–4 | Главная (12 секций), тарифы, демо, о нас, контакты, header/footer | **Done** |
| 3 — Продукты/решения | Нед. 4–6 | 6 продуктовых + 8 вертикальных + интеграции (data-driven) | **Done** |
| 4 — Конверсия | Нед. 6–8 | 5 кейсов + hub, блог (10 статей, 9 категорий) | **Частично** (кейсы+блог done; ROI-калькулятор, конфигуратор — todo) |
| 5 — Автоматизация | Нед. 8–10 | Битрикс24 Open Lines бот, If-So, nurture-цепочки, A/B | Не начата |
| 6 — Оптимизация | Постоянно | A/B-тесты, SEO-контент, новые кейсы, PageSpeed | Не начата |

Детальный план: `agent_docs/implementation-plan.md`
