<?php
/**
 * Template: Mobile Pass Bureau v2 / Мобильное бюро пропусков (NEW)
 * Combines strengths of local template + live page (pass24online.ru/products/mbp)
 *
 * Added vs v1:
 *  - 3-benefita bar (безопасность / конфликты / экономия)
 *  - До/После метрики с конкретными числами
 *  - Секция 3-персон (УК, жители, охрана)
 *  - Логотипы клиентов
 *  - Расширенные use-cases (8 сегментов)
 *
 * @package PASS24_Child
 */
defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/product-data.php';
require_once PASS24_CHILD_DIR . '/inc/integration-data.php';

$product = pass24_get_product( 'mobile-pass-bureau' );
if ( ! $product ) {
	wp_redirect( home_url( '/products/' ) );
	exit;
}

$all_integrations = pass24_get_integrations();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width:100%;">
		<main class="site-main">


			<!-- ================================================================
			     1. HERO  (локальная: сильный заголовок + бейдж с метрикой)
			     ================================================================ -->

			<section class="p24-section p24-product-hero">
				<div class="p24-container">
					<div class="p24-product-hero__layout">

						<div class="p24-product-hero__content">
							<span class="p24-badge p24-badge-accent"><?php echo $product['badge']; ?></span>
							<h1 class="p24-h1"><?php echo $product['hero_title']; ?></h1>
							<p class="p24-subtitle"><?php echo $product['hero_subtitle']; ?></p>

							<div class="p24-product-hero__actions">
								<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Начать пилот — 14 дней</a>
								<a href="#demo-video" class="p24-btn p24-btn-secondary p24-btn-lg">Смотреть видео</a>
							</div>

							<ul class="p24-cta-checklist">
								<li>Развёртывание за&nbsp;1&nbsp;час</li>
								<li>Обучение включено</li>
								<li>Без замены оборудования</li>
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
			     2. 3 БЕНЕФИТА  (живая: ключевые обещания продукта)
			     ================================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-mbp-benefits">

						<div class="p24-mbp-benefit">
							<div class="p24-mbp-benefit__icon" aria-hidden="true">🔒</div>
							<h3 class="p24-h4">Повышаем безопасность</h3>
							<p>Цифровой журнал и&nbsp;история проходов. Чёрные списки. Уведомления при нарушениях.</p>
						</div>

						<div class="p24-mbp-benefit">
							<div class="p24-mbp-benefit__icon" aria-hidden="true">✅</div>
							<h3 class="p24-h4">Устраняем конфликты на&nbsp;въезде</h3>
							<p>Охрана видит пропуск заранее. Гость не&nbsp;стоит в&nbsp;очереди. Данные не&nbsp;теряются.</p>
						</div>

						<div class="p24-mbp-benefit">
							<div class="p24-mbp-benefit__icon" aria-hidden="true">⏱</div>
							<h3 class="p24-h4">Экономим ваше время</h3>
							<p>Пропуск оформляется за&nbsp;30&nbsp;секунд вместо&nbsp;2&nbsp;минут. Охрана не&nbsp;заполняет бумаги.</p>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     3. ДО / ПОСЛЕ  (живая: конкретные числа — главное оружие)
			     ================================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Что изменится после внедрения</h2>
						<p class="p24-subtitle">Реальные цифры от&nbsp;объектов на&nbsp;PASS24</p>
					</div>

					<div class="p24-mbp-before-after">

						<div class="p24-mbp-ba-item">
							<div class="p24-mbp-ba-item__label">Оформление пропуска гостю</div>
							<div class="p24-mbp-ba-item__row">
								<div class="p24-mbp-ba-item__before">
									<span class="p24-mbp-ba-item__value p24-mbp-ba-item__value--before">2&nbsp;мин</span>
									<span class="p24-mbp-ba-item__caption">было</span>
								</div>
								<div class="p24-mbp-ba-item__arrow">→</div>
								<div class="p24-mbp-ba-item__after">
									<span class="p24-mbp-ba-item__value p24-mbp-ba-item__value--after">30&nbsp;сек</span>
									<span class="p24-mbp-ba-item__caption">стало</span>
								</div>
							</div>
						</div>

						<div class="p24-mbp-ba-item">
							<div class="p24-mbp-ba-item__label">Время охраны на&nbsp;заполнение журнала</div>
							<div class="p24-mbp-ba-item__row">
								<div class="p24-mbp-ba-item__before">
									<span class="p24-mbp-ba-item__value p24-mbp-ba-item__value--before">до&nbsp;2&nbsp;ч/день</span>
									<span class="p24-mbp-ba-item__caption">было</span>
								</div>
								<div class="p24-mbp-ba-item__arrow">→</div>
								<div class="p24-mbp-ba-item__after">
									<span class="p24-mbp-ba-item__value p24-mbp-ba-item__value--after">0</span>
									<span class="p24-mbp-ba-item__caption">стало</span>
								</div>
							</div>
						</div>

						<div class="p24-mbp-ba-item">
							<div class="p24-mbp-ba-item__label">Проверка автомобиля на&nbsp;КПП</div>
							<div class="p24-mbp-ba-item__row">
								<div class="p24-mbp-ba-item__before">
									<span class="p24-mbp-ba-item__value p24-mbp-ba-item__value--before">до&nbsp;1&nbsp;мин</span>
									<span class="p24-mbp-ba-item__caption">было</span>
								</div>
								<div class="p24-mbp-ba-item__arrow">→</div>
								<div class="p24-mbp-ba-item__after">
									<span class="p24-mbp-ba-item__value p24-mbp-ba-item__value--after">2&nbsp;сек</span>
									<span class="p24-mbp-ba-item__caption">стало</span>
								</div>
							</div>
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
						<p class="p24-subtitle">Три простых шага для начала работы</p>
					</div>
					<div class="p24-product-steps">

						<div class="p24-product-step">
							<div class="p24-product-step__num">01</div>
							<h3 class="p24-h4">Житель создаёт пропуск</h3>
							<p>Через мобильное приложение или веб&#8209;интерфейс за&nbsp;5&nbsp;секунд. Указывает имя гостя и&nbsp;дату визита. Или отправляет гостю ссылку для&nbsp;самостоятельного заполнения. Работает с&nbsp;Яндекс&nbsp;Алисой.</p>
						</div>

						<div class="p24-product-step">
							<div class="p24-product-step__num">02</div>
							<h3 class="p24-h4">Гость получает QR&#8209;код</h3>
							<p>Ссылка приходит в&nbsp;WhatsApp, Telegram или SMS. Никаких приложений&nbsp;&mdash; гость открывает QR прямо в&nbsp;браузере. Данные пропуска автоматически передаются на&nbsp;пост охраны.</p>
						</div>

						<div class="p24-product-step">
							<div class="p24-product-step__num">03</div>
							<h3 class="p24-h4">Проход по&nbsp;QR на&nbsp;КПП</h3>
							<p>Охрана сканирует код или шлагбаум открывается автоматически. Идентификация по&nbsp;QR, NFC, лицу или номеру авто. Данные фиксируются в&nbsp;журнале.</p>
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
			     6. ДЛЯ КОГО  (живая: 3 персоны — УК, жители, охрана)
			     ================================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Объединяет все стороны</h2>
						<p class="p24-subtitle">Каждый участник получает своё</p>
					</div>
					<div class="p24-mbp-personas">

						<div class="p24-card p24-mbp-persona">
							<div class="p24-mbp-persona__role">УК и&nbsp;владельцы</div>
							<h3 class="p24-h4">Полный контроль объекта</h3>
							<ul class="p24-mbp-persona__list">
								<li>Настройка прав доступа по&nbsp;ролям</li>
								<li>Списки нарушителей и&nbsp;чёрные списки</li>
								<li>Интеграция с&nbsp;видеонаблюдением</li>
								<li>Отчёты по&nbsp;трафику и&nbsp;посещаемости</li>
							</ul>
						</div>

						<div class="p24-card p24-mbp-persona">
							<div class="p24-mbp-persona__role">Жители и&nbsp;арендаторы</div>
							<h3 class="p24-h4">Пропуск за&nbsp;2&nbsp;клика</h3>
							<ul class="p24-mbp-persona__list">
								<li>Мобильное приложение и&nbsp;веб&#8209;версия</li>
								<li>Гость получает QR&nbsp;в&nbsp;мессенджер</li>
								<li>Повторные гости — автозаполнение</li>
								<li>Массовые пропуска для&nbsp;мероприятий</li>
							</ul>
						</div>

						<div class="p24-card p24-mbp-persona">
							<div class="p24-mbp-persona__role">Служба охраны</div>
							<h3 class="p24-h4">Ничего не&nbsp;нужно заполнять</h3>
							<ul class="p24-mbp-persona__list">
								<li>Электронный журнал вместо бумажного</li>
								<li>Список ожидаемых посетителей заранее</li>
								<li>Сканирование QR за&nbsp;2&nbsp;секунды</li>
								<li>Уведомления о&nbsp;нарушениях в&nbsp;реальном времени</li>
							</ul>
						</div>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     7. ИНТЕГРАЦИИ  (локальная)
			     ================================================================ -->

			<?php if ( ! empty( $product['integrations'] ) ) : ?>
			<section class="p24-section">
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
			     8. КОМУ ПОДХОДИТ  (расширено: локальные 4 + живая добавила сегменты)
			     ================================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Кому подходит</h2>
					</div>
					<div class="p24-grid-2">

						<?php foreach ( $product['use_cases'] as $uc ) : ?>
						<a href="<?php echo esc_url( $uc['url'] ); ?>" class="p24-card p24-product-usecase">
							<h3 class="p24-h4"><?php echo esc_html( $uc['segment'] ); ?></h3>
							<p><?php echo $uc['desc']; ?></p>
							<span class="p24-product-usecase__link">Подробнее &rarr;</span>
						</a>
						<?php endforeach; ?>

						<a href="/solutions/logistics/" class="p24-card p24-product-usecase">
							<h3 class="p24-h4">Складские комплексы</h3>
							<p>Учёт грузового транспорта и&nbsp;персонала подрядчиков. Автоматический пропуск по&nbsp;номеру авто.</p>
							<span class="p24-product-usecase__link">Подробнее &rarr;</span>
						</a>

						<a href="/solutions/industrial-park/" class="p24-card p24-product-usecase">
							<h3 class="p24-h4">Производство и&nbsp;стройка</h3>
							<p>Учёт рабочего времени субподрядчиков. Зонирование доступа по&nbsp;категориям персонала.</p>
							<span class="p24-product-usecase__link">Подробнее &rarr;</span>
						</a>

					</div>
				</div>
			</section>


			<!-- ================================================================
			     9. НАМ ДОВЕРЯЮТ  (живая: логотипы клиентов — социальное доказательство)
			     ================================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Нам доверяют</h2>
						<p class="p24-subtitle">Более 300 объектов по&nbsp;всей России</p>
					</div>
					<div class="p24-mbp-clients">
						<?php
						$clients = [
							'ПИК Группа',
							'PSN Group',
							'Villagio Estate',
							'Садовые Кварталы',
							'Сады Майендорф',
							'ЖК Розмарин',
							'CEO ROOMS',
							'Мартемьяново',
						];
						foreach ( $clients as $client ) :
						?>
						<div class="p24-mbp-client">
							<span class="p24-mbp-client__name"><?php echo esc_html( $client ); ?></span>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ================================================================
			     10. КЕЙС  (локальная: Садовые Кварталы — 70% снижение звонков)
			     ================================================================ -->

			<?php if ( ! empty( $product['case_study']['client'] ) ) : ?>
			<section class="p24-section p24-section-gray">
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
			     11. ФИНАЛЬНЫЙ CTA  (локальная: тёмный блок с пилотом)
			     ================================================================ -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align:center;max-width:700px;">
					<h2 class="p24-h2" style="color:#fff;">Попробуйте <?php echo esc_html( $product['name'] ); ?></h2>
					<p class="p24-subtitle" style="color:rgba(255,255,255,.7);margin-bottom:32px;">
						14 дней бесплатно. Обучение и&nbsp;онлайн&#8209;сопровождение включены.
					</p>
					<div style="display:flex;gap:16px;justify-content:center;flex-wrap:wrap;">
						<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот — 14 дней</a>
						<a href="/pricing/" class="p24-btn p24-btn-ghost p24-btn-lg">Тарифы</a>
					</div>
					<ul class="p24-cta-checklist" style="justify-content:center;margin-top:24px;color:rgba(255,255,255,.6);">
						<li>Без замены оборудования</li>
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
