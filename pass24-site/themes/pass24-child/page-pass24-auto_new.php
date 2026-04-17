<?php
/**
 * Template: PASS24.авто v2
 * Combines strengths of local template + live page (pass24online.ru/products/pass24auto)
 *
 * Added vs v1:
 *  - Состав решения "под ключ" (4 пункта комплекта)
 *  - Метрики доверия (8+ лет, 400+ проектов, 5x скорость, 99.99% uptime)
 *  - Установка за 24 часа
 *  - Расширенный список 9 возможностей
 *  - Блок техподдержки (ИБП, опечатанный шкаф, удалённый мониторинг)
 *  - Таблица цен ПАК Лайт / ПАК Базовый с рассрочкой
 *
 * @package PASS24_Child
 */
defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/product-data.php';
require_once PASS24_CHILD_DIR . '/inc/integration-data.php';

$product = pass24_get_product( 'pass24-auto' );
if ( ! $product ) {
	wp_redirect( home_url( '/products/' ) );
	exit;
}

$all_integrations = pass24_get_integrations();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();

// ── JSON-LD Schema ────────────────────────────────────────────────────────────
$schema = [
	'@context' => 'https://schema.org',
	'@graph'   => [

		[
			'@type'       => 'Product',
			'name'        => 'PASS24.авто',
			'description' => 'Программно-аппаратный комплекс для автоматического распознавания государственных номерных знаков автомобилей (LPR/ANPR) и управления шлагбаумом без участия охраны. Точность распознавания 98%, скорость — 0,5 секунды.',
			'brand'       => [ '@type' => 'Brand', 'name' => 'PASS24' ],
			'url'         => 'https://pass24online.ru/products/pass24auto',
			'offers'      => [
				'@type'         => 'AggregateOffer',
				'lowPrice'      => '288000',
				'highPrice'     => '338400',
				'priceCurrency' => 'RUB',
				'offerCount'    => '2',
			],
		],

		[
			'@type'    => 'BreadcrumbList',
			'itemListElement' => [
				[ '@type' => 'ListItem', 'position' => 1, 'name' => 'Главная',  'item' => 'https://pass24online.ru/' ],
				[ '@type' => 'ListItem', 'position' => 2, 'name' => 'Продукты', 'item' => 'https://pass24online.ru/products/' ],
				[ '@type' => 'ListItem', 'position' => 3, 'name' => 'PASS24.авто', 'item' => 'https://pass24online.ru/products/pass24auto' ],
			],
		],

		[
			'@type' => 'HowTo',
			'name'  => 'Как работает система распознавания номеров PASS24.авто',
			'step'  => [
				[
					'@type'  => 'HowToStep',
					'position' => 1,
					'name'   => 'Регистрация номера автомобиля',
					'text'   => 'Житель или УК добавляет государственный номер в систему через мобильное приложение, Telegram, web-форму или Яндекс Алису — менее чем за 15 секунд.',
				],
				[
					'@type'  => 'HowToStep',
					'position' => 2,
					'name'   => 'Автоматическое распознавание номера камерой',
					'text'   => 'При подъезде к КПП ИК-камера считывает государственный номер за 0,5 секунды с точностью 98%. Поддерживаются номера РФ, СНГ и Евросоюза.',
				],
				[
					'@type'  => 'HowToStep',
					'position' => 3,
					'name'   => 'Автоматическое открытие шлагбаума',
					'text'   => 'Если номер зарегистрирован — шлагбаум открывается без участия охраны. Система фиксирует событие въезда/выезда и отправляет уведомление в приложение.',
				],
			],
		],

		[
			'@type'      => 'FAQPage',
			'mainEntity' => [
				[
					'@type'          => 'Question',
					'name'           => 'Что такое PASS24.авто?',
					'acceptedAnswer' => [ '@type' => 'Answer', 'text' => 'PASS24.авто — программно-аппаратный комплекс (ПАК) для автоматического распознавания государственных номерных знаков (LPR/ANPR) и управления въездом на территорию. Комплект включает 2 ИК-камеры с сервером, инфраструктурное оборудование, монтаж и интеграцию с облачной СКУД PASS24.' ],
				],
				[
					'@type'          => 'Question',
					'name'           => 'Какова точность распознавания автомобильных номеров?',
					'acceptedAnswer' => [ '@type' => 'Answer', 'text' => 'Точность распознавания государственных номерных знаков составляет 98%. Система работает в любых условиях: ночью, в дождь, снег и при загрязнённых номерах — благодаря инфракрасным камерам.' ],
				],
				[
					'@type'          => 'Question',
					'name'           => 'Сколько стоит PASS24.авто?',
					'acceptedAnswer' => [ '@type' => 'Answer', 'text' => 'Стоимость ПАК Лайт — 288 000 рублей (или 28 800 ₽/мес × 12). ПАК Базовый с монтажом на объекте — 338 400 рублей (или 33 840 ₽/мес × 12). В обоих вариантах доступна рассрочка на 12 месяцев.' ],
				],
				[
					'@type'          => 'Question',
					'name'           => 'За сколько времени устанавливается система распознавания номеров?',
					'acceptedAnswer' => [ '@type' => 'Answer', 'text' => 'Монтаж и настройка PASS24.авто занимают 24 часа. Обучение персонала включено в стоимость. Замена существующего оборудования не требуется.' ],
				],
				[
					'@type'          => 'Question',
					'name'           => 'С каким оборудованием совместим PASS24.авто?',
					'acceptedAnswer' => [ '@type' => 'Answer', 'text' => 'PASS24.авто интегрируется с ведущими системами видеонаблюдения и СКУД: Trassir, Dahua, Sigur, CVS, АвтоМаршал. Система работает с существующими шлагбаумами и контроллерами — замена оборудования не нужна.' ],
				],
				[
					'@type'          => 'Question',
					'name'           => 'Работает ли система в ночное время и при плохом освещении?',
					'acceptedAnswer' => [ '@type' => 'Answer', 'text' => 'Да. В комплекте используются инфракрасные камеры, которые обеспечивают чёткое распознавание номеров даже в полной темноте, при дожде, снеге и загрязнённых номерных знаках.' ],
				],
			],
		],

	],
];
?>
<script type="application/ld+json"><?php echo wp_json_encode( $schema, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT ); ?></script>
<?php

// ── End JSON-LD ───────────────────────────────────────────────────────────────
?>

<div class="site-content" id="content">
	<div class="content-area" style="width:100%;">
		<main class="site-main">


			<!-- ================================================================
			     1. HERO  (локальная: конкретный заголовок + бейдж с точностью)
			     ================================================================ -->

			<section class="p24-section p24-product-hero">
				<div class="p24-container">
					<div class="p24-product-hero__layout">

						<div class="p24-product-hero__content">
							<span class="p24-badge p24-badge-accent"><?php echo $product['badge']; ?></span>
							<h1 class="p24-h1"><?php echo $product['hero_title']; ?></h1>
							<p class="p24-subtitle"><?php echo $product['hero_subtitle']; ?></p>
							<p class="p24-auto-definition">
								<strong>PASS24.авто</strong> — система автоматического распознавания государственных номерных знаков (LPR&nbsp;/&nbsp;ANPR) для жилых комплексов, бизнес&#8209;центров и&nbsp;складских комплексов. Шлагбаум открывается без&nbsp;участия охраны.
							</p>

							<div class="p24-product-hero__actions">
								<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Начать пилот — 14 дней</a>
								<a href="#pak-pricing" class="p24-btn p24-btn-secondary p24-btn-lg">Стоимость ПАК</a>
							</div>

							<ul class="p24-cta-checklist">
								<li>Установка за&nbsp;24&nbsp;часа</li>
								<li>Без замены оборудования</li>
								<li>Техподдержка включена</li>
							</ul>
						</div>

						<div class="p24-product-hero__media">
							<div class="p24-product-hero__image-wrap">
								<img
									src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/products/' . $product['hero_image'] ); ?>"
									alt="<?php echo esc_attr( $product['name'] ); ?>"
									loading="eager"
									width="600" height="400"
									onerror="this.closest('.p24-product-hero__media').style.display='none'"
								>
							</div>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     2. МЕТРИКИ ДОВЕРИЯ  (живая: 8+ лет / 400+ проектов / 5x / 98%)
			     ================================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-auto-stats">

						<div class="p24-auto-stat">
							<div class="p24-auto-stat__value">8+</div>
							<div class="p24-auto-stat__label">лет на&nbsp;рынке систем безопасности</div>
						</div>

						<div class="p24-auto-stat">
							<div class="p24-auto-stat__value">400+</div>
							<div class="p24-auto-stat__label">успешно реализованных проектов</div>
						</div>

						<div class="p24-auto-stat">
							<div class="p24-auto-stat__value">5×</div>
							<div class="p24-auto-stat__label">увеличение пропускной способности&nbsp;КПП</div>
						</div>

						<div class="p24-auto-stat">
							<div class="p24-auto-stat__value">98%</div>
							<div class="p24-auto-stat__label">точность распознавания номеров</div>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     3. СОСТАВ РЕШЕНИЯ "ПОД КЛЮЧ"  (живая: что входит в ПАК)
			     ================================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Готовое решение «под&nbsp;ключ»</h2>
						<p class="p24-subtitle">Всё необходимое для&nbsp;автоматизации въезда в&nbsp;одном комплекте</p>
					</div>
					<div class="p24-auto-bundle">

						<div class="p24-card p24-auto-bundle__item">
							<div class="p24-auto-bundle__num">01</div>
							<h3 class="p24-h4">2 камеры с&nbsp;распознаванием</h3>
							<p>ИК&#8209;камеры с&nbsp;сервером «на&nbsp;борту» на&nbsp;въезд и&nbsp;выезд. Работают в&nbsp;полной темноте, дождь, снег, загрязнённые номера.</p>
						</div>

						<div class="p24-card p24-auto-bundle__item">
							<div class="p24-auto-bundle__num">02</div>
							<h3 class="p24-h4">Инфраструктурное оборудование</h3>
							<p>Источники бесперебойного питания, опечатанные шкафы защиты оборудования. Работа без перебоев даже при отключении электричества.</p>
						</div>

						<div class="p24-card p24-auto-bundle__item">
							<div class="p24-auto-bundle__num">03</div>
							<h3 class="p24-h4">Пуско-наладка и&nbsp;обучение</h3>
							<p>Монтаж и&nbsp;настройка за&nbsp;24&nbsp;часа. Обучение персонала включено. Удалённый мониторинг — мы первыми узнаем о&nbsp;неисправности.</p>
						</div>

						<div class="p24-card p24-auto-bundle__item">
							<div class="p24-auto-bundle__num">04</div>
							<h3 class="p24-h4">Интеграция с&nbsp;МБП PASS24</h3>
							<p>Пропуска из&nbsp;мобильного приложения, Telegram, web&#8209;формы или Яндекс&nbsp;Алисы автоматически передаются в&nbsp;систему распознавания.</p>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     4. КАК ЭТО РАБОТАЕТ  (локальная шаги + детали из живой)
			     ================================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Как это работает</h2>
						<p class="p24-subtitle">От&nbsp;заявки до&nbsp;открытого шлагбаума — полностью автоматически</p>
					</div>
					<div class="p24-product-steps">

						<div class="p24-product-step">
							<div class="p24-product-step__num">01</div>
							<h3 class="p24-h4">Регистрация номера</h3>
							<p>Житель или УК добавляет госномер в&nbsp;систему менее чем за&nbsp;15&nbsp;секунд: через приложение, Telegram, web&#8209;форму или Яндекс&nbsp;Алису. Можно привязать несколько авто к&nbsp;одному аккаунту.</p>
						</div>

						<div class="p24-product-step">
							<div class="p24-product-step__num">02</div>
							<h3 class="p24-h4">Камера распознаёт номер</h3>
							<p>При подъезде ИК&#8209;камера считывает номер за&nbsp;0,5&nbsp;сек с&nbsp;точностью 98%. Работает с&nbsp;номерами РФ, СНГ и&nbsp;Евросоюза. Фиксирует фото и&nbsp;видео каждого проезда.</p>
						</div>

						<div class="p24-product-step">
							<div class="p24-product-step__num">03</div>
							<h3 class="p24-h4">Шлагбаум открывается</h3>
							<p>Если номер в&nbsp;базе&nbsp;&mdash; проезд без&nbsp;остановки. Система создаёт событие о&nbsp;прибытии/убытии, уведомляет в&nbsp;приложении и&nbsp;дублирует данные в&nbsp;электронный журнал охраны.</p>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     5. КЛЮЧЕВЫЕ ВОЗМОЖНОСТИ  (локальная: 4 фичи с метриками)
			     ================================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Ключевые возможности</h2>
					</div>
					<div class="p24-grid-2">
						<?php foreach ( $product['features'] as $feature ) : ?>
						<div class="p24-card p24-product-feature">
							<div class="p24-product-feature__metric"><?php echo $feature['metric']; ?></div>
							<h3 class="p24-h4"><?php echo $feature['title']; ?></h3>
							<p><?php echo $feature['desc']; ?></p>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ================================================================
			     6. 9 ВОЗМОЖНОСТЕЙ СИСТЕМЫ  (живая: детальный список)
			     ================================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Что умеет система распознавания номеров?</h2>
					</div>
					<div class="p24-auto-features-list">

						<?php
						$system_features = [
							[ 'title' => 'Номера РФ, СНГ и&nbsp;Евросоюза',          'desc' => 'Распознавание номерных знаков всех форматов.' ],
							[ 'title' => 'Фото&#8209;видео фиксация',                  'desc' => 'Каждый проезд записывается с&nbsp;визуальным подтверждением.' ],
							[ 'title' => 'Гостевые авто по&nbsp;QR&nbsp;+ номеру',     'desc' => 'Житель указывает номер гостя при создании пропуска.' ],
							[ 'title' => 'Уведомления в&nbsp;реальном времени',        'desc' => 'Push в&nbsp;приложение при каждом проезде зарегистрированного авто.' ],
							[ 'title' => 'Контроль типов транспорта',                  'desc' => 'Разделение: жители, гости, курьеры, грузовой транспорт.' ],
							[ 'title' => 'Фиксация несанкционированных попыток',       'desc' => 'Журнал отказов с&nbsp;фото и&nbsp;уведомлением охраны.' ],
							[ 'title' => 'Электронный журнал охраны',                  'desc' => 'Все события дублируются автоматически, бессрочное хранение.' ],
							[ 'title' => 'Отчёты о&nbsp;ручном управлении',            'desc' => 'Фиксация всех случаев, когда шлагбаум открыт вручную.' ],
							[ 'title' => 'Управление и&nbsp;мониторинг онлайн',        'desc' => 'Вход из&nbsp;любой точки. Доступность системы 99,99%.' ],
						];
						foreach ( $system_features as $f ) :
						?>
						<div class="p24-auto-feature-item">
							<div class="p24-auto-feature-item__check" aria-hidden="true">✓</div>
							<div>
								<strong><?php echo $f['title']; ?></strong>
								<p><?php echo $f['desc']; ?></p>
							</div>
						</div>
						<?php endforeach; ?>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     7. ТЕХПОДДЕРЖКА И НАДЁЖНОСТЬ  (живая: ИБП, шкаф, мониторинг)
			     ================================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Надёжность и&nbsp;техподдержка</h2>
						<p class="p24-subtitle">Система работает 24/7 — даже когда отключают свет</p>
					</div>
					<div class="p24-grid-2">

						<div class="p24-card">
							<h3 class="p24-h4">Техподдержка включена</h3>
							<p>Круглосуточная поддержка без доплат. Удалённый мониторинг&nbsp;&mdash; мы первыми узнаем о&nbsp;неисправности и&nbsp;устраним до&nbsp;вашего обращения.</p>
						</div>

						<div class="p24-card">
							<h3 class="p24-h4">Источники бесперебойного питания</h3>
							<p>ИБП гарантируют работу оборудования при перебоях с&nbsp;электроэнергией. КПП не&nbsp;остановится из-за&nbsp;отключения света.</p>
						</div>

						<div class="p24-card">
							<h3 class="p24-h4">Защищённые шкафы</h3>
							<p>Всё оборудование установлено в&nbsp;опечатанные защитные шкафы. Несанкционированный доступ фиксируется и&nbsp;блокируется.</p>
						</div>

						<div class="p24-card">
							<h3 class="p24-h4">Данные в&nbsp;российском ЦОД</h3>
							<p>Доступность 99,99%. Автоматические бэкапы. Соответствие 152&#8209;ФЗ. Входим в&nbsp;Единый реестр российского ПО.</p>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     8. ИНТЕГРАЦИИ  (локальная)
			     ================================================================ -->

			<?php if ( ! empty( $product['integrations'] ) ) : ?>
			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Совместимые интеграции</h2>
						<p class="p24-subtitle">Работает с&nbsp;вашим оборудованием</p>
					</div>
					<div class="p24-product-integrations">
						<?php foreach ( $product['integrations'] as $int_slug ) :
							$int = $all_integrations[ $int_slug ] ?? null;
							if ( ! $int ) continue;
						?>
						<div class="p24-product-integration">
							<img
								src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/integrations/' . $int['logo'] ); ?>"
								alt="<?php echo esc_attr( $int['name'] ); ?>"
								width="120" height="48"
								loading="lazy"
							>
							<span class="p24-small"><?php echo esc_html( $int['name'] ); ?></span>
						</div>
						<?php endforeach; ?>
					</div>
					<div style="text-align:center;margin-top:32px;">
						<a href="/integrations/" class="p24-btn p24-btn-secondary">Все интеграции</a>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ================================================================
			     9. КОМУ ПОДХОДИТ  (локальная: 4 сегмента)
			     ================================================================ -->

			<?php if ( ! empty( $product['use_cases'] ) ) : ?>
			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Для каких объектов подходит PASS24.авто?</h2>
					</div>
					<div class="p24-grid-2">
						<?php foreach ( $product['use_cases'] as $uc ) : ?>
						<a href="<?php echo esc_url( $uc['url'] ); ?>" class="p24-card p24-product-usecase">
							<h3 class="p24-h4"><?php echo esc_html( $uc['segment'] ); ?></h3>
							<p><?php echo $uc['desc']; ?></p>
							<span class="p24-product-usecase__link">Подробнее &rarr;</span>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ================================================================
			     10. ЦЕНЫ ПАК  (живая: ПАК Лайт / ПАК Базовый с рассрочкой)
			     ================================================================ -->

			<section class="p24-section p24-section-gray" id="pak-pricing">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Стоимость комплекта</h2>
						<p class="p24-subtitle">Единоразовый платёж или рассрочка на&nbsp;12&nbsp;месяцев</p>
					</div>
					<div class="p24-auto-pricing">

						<div class="p24-card p24-auto-pricing__card">
							<div class="p24-badge p24-badge-accent" style="margin-bottom:12px;">Новый</div>
							<h3 class="p24-h3">ПАК Лайт</h3>
							<ul class="p24-auto-pricing__list">
								<li>2 камеры с&nbsp;распознаванием и&nbsp;сервером</li>
								<li>Инфраструктурное оборудование</li>
								<li>Удалённые пуско-наладочные работы</li>
								<li>Интеграция с&nbsp;PASS24.online</li>
							</ul>
							<div class="p24-auto-pricing__price">
								<div class="p24-auto-pricing__price-main">288&nbsp;000&nbsp;₽</div>
								<div class="p24-auto-pricing__price-sub">или 28&nbsp;800&nbsp;₽/мес × 12</div>
							</div>
							<a href="/demo/" class="p24-btn p24-btn-primary" style="width:100%;">Оставить заявку</a>
						</div>

						<div class="p24-card p24-auto-pricing__card">
							<h3 class="p24-h3">ПАК Базовый</h3>
							<ul class="p24-auto-pricing__list">
								<li>2 камеры с&nbsp;распознаванием и&nbsp;сервером</li>
								<li>Инфраструктурное оборудование</li>
								<li>Пуско-наладка и&nbsp;обучение на&nbsp;объекте</li>
								<li>Интеграция с&nbsp;PASS24.online</li>
							</ul>
							<div class="p24-auto-pricing__price">
								<div class="p24-auto-pricing__price-main">338&nbsp;400&nbsp;₽</div>
								<div class="p24-auto-pricing__price-sub">или 33&nbsp;840&nbsp;₽/мес × 12</div>
							</div>
							<a href="/demo/" class="p24-btn p24-btn-primary" style="width:100%;">Оставить заявку</a>
						</div>

					</div>
					<p class="p24-small" style="text-align:center;margin-top:16px;color:var(--p24-text-muted);">
						* Стоимость монтажа зависит от&nbsp;конфигурации объекта. Уточняйте у&nbsp;менеджера.
					</p>
				</div>
			</section>


			<!-- ================================================================
			     11. КЕЙС  (локальная: Агаларов Эстейт)
			     ================================================================ -->

			<?php if ( ! empty( $product['case_study']['client'] ) ) : ?>
			<section class="p24-section">
				<div class="p24-container" style="max-width:800px;">
					<div class="p24-product-case">
						<span class="p24-badge p24-badge-primary"><?php echo esc_html( $product['case_study']['segment'] ); ?></span>
						<h2 class="p24-h3" style="margin-top:12px;"><?php echo esc_html( $product['case_study']['client'] ); ?></h2>
						<blockquote class="p24-product-case__quote">
							<?php echo $product['case_study']['quote']; ?>
						</blockquote>
						<a href="<?php echo esc_url( $product['case_study']['url'] ); ?>" class="p24-btn p24-btn-secondary">
							Читать кейс &rarr;
						</a>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ================================================================
			     12. FAQ  (AEO: прямые ответы на вопросы пользователей)
			     ================================================================ -->

			<section class="p24-section p24-section-gray" id="faq">
				<div class="p24-container" style="max-width:800px;">
					<div class="p24-section-header">
						<h2 class="p24-h2">Частые вопросы о&nbsp;PASS24.авто</h2>
					</div>
					<div class="p24-faq" data-p24-faq>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Что такое PASS24.авто?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
							</button>
							<div class="p24-faq__answer">
								<p>PASS24.авто&nbsp;&mdash; программно-аппаратный комплекс (ПАК) для автоматического распознавания государственных номерных знаков (LPR&nbsp;/&nbsp;ANPR) и&nbsp;управления въездом на&nbsp;территорию. Комплект включает 2&nbsp;ИК&#8209;камеры с&nbsp;сервером, инфраструктурное оборудование, монтаж и&nbsp;интеграцию с&nbsp;облачной СКУД PASS24. Шлагбаум открывается автоматически без&nbsp;участия охраны.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Какова точность распознавания автомобильных номеров?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
							</button>
							<div class="p24-faq__answer">
								<p>Точность распознавания государственных номерных знаков составляет <strong>98%</strong>. Система работает в&nbsp;любых условиях: ночью, в&nbsp;дождь, снег и&nbsp;при загрязнённых номерах&nbsp;&mdash; благодаря инфракрасным камерам со&nbsp;встроенной подсветкой. Скорость считывания — <strong>0,5&nbsp;секунды</strong>.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Сколько стоит система распознавания номеров PASS24.авто?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
							</button>
							<div class="p24-faq__answer">
								<p>Стоимость <strong>ПАК Лайт</strong>&nbsp;&mdash; 288&nbsp;000&nbsp;₽ (или рассрочка 28&nbsp;800&nbsp;₽/мес × 12). <strong>ПАК Базовый</strong> с&nbsp;выездным монтажом&nbsp;&mdash; 338&nbsp;400&nbsp;₽ (или 33&nbsp;840&nbsp;₽/мес × 12). В&nbsp;обоих вариантах включены оборудование, настройка и&nbsp;интеграция с&nbsp;PASS24.online.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								За сколько времени устанавливается система?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
							</button>
							<div class="p24-faq__answer">
								<p>Монтаж и&nbsp;настройка занимают <strong>24&nbsp;часа</strong>. Обучение персонала и&nbsp;удалённый мониторинг включены в&nbsp;стоимость. Замена существующего оборудования&nbsp;&mdash; шлагбаумов, камер, контроллеров&nbsp;&mdash; не&nbsp;требуется.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								С каким оборудованием совместим PASS24.авто?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
							</button>
							<div class="p24-faq__answer">
								<p>PASS24.авто интегрируется с&nbsp;ведущими СКУД и&nbsp;системами видеоаналитики: <strong>Trassir, Dahua, Sigur, CVS, АвтоМаршал</strong>. Система работает с&nbsp;существующими шлагбаумами и&nbsp;контроллерами доступа через открытый API.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Работает ли система ночью и&nbsp;при плохом освещении?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
							</button>
							<div class="p24-faq__answer">
								<p>Да. В&nbsp;комплекте используются инфракрасные камеры с&nbsp;активной ИК&#8209;подсветкой. Чёткое распознавание номеров обеспечивается в&nbsp;полной темноте, при дожде, снеге и&nbsp;при загрязнённых номерных знаках&nbsp;&mdash; без&nbsp;снижения точности.</p>
							</div>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     13. ФИНАЛЬНЫЙ CTA  (локальная: тёмный блок)
			     ================================================================ -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align:center;max-width:700px;">
					<h2 class="p24-h2" style="color:#fff;">Попробуйте <?php echo esc_html( $product['name'] ); ?></h2>
					<p class="p24-subtitle" style="color:rgba(255,255,255,.7);margin-bottom:32px;">
						14 дней бесплатно. Обучение и&nbsp;онлайн&#8209;сопровождение включены.
					</p>
					<div style="display:flex;gap:16px;justify-content:center;flex-wrap:wrap;">
						<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот — 14 дней</a>
						<a href="#pak-pricing" class="p24-btn p24-btn-ghost p24-btn-lg">Стоимость ПАК</a>
					</div>
					<ul class="p24-cta-checklist" style="justify-content:center;margin-top:24px;color:rgba(255,255,255,.6);">
						<li>Установка за&nbsp;24&nbsp;часа</li>
						<li>Серверы в&nbsp;РФ</li>
						<li>152&#8209;ФЗ</li>
					</ul>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
