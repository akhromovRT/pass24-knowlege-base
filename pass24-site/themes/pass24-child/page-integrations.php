<?php
/**
 * Template: Integrations / Интеграции
 *
 * Страница /integrations/ — партнёры, Open API, CTA для интеграторов.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/integration-data.php';
$integrations = pass24_get_integrations();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- ============================================================
			     HERO
			     ============================================================ -->

			<section class="p24-integrations-hero">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<h1 class="p24-h1" style="margin-bottom: 16px;">Интеграции</h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
						PASS24 работает с&nbsp;ведущими производителями оборудования для видеонаблюдения и&nbsp;контроля доступа
					</p>
				</div>
			</section>


			<!-- ============================================================
			     INTEGRATIONS GRID
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-integrations-grid">
						<?php foreach ( $integrations as $slug => $int ) : ?>
						<div class="p24-card p24-integration-card">
							<img
								class="p24-integration-card__logo"
								src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/integrations/' . $int['logo'] ); ?>"
								alt="<?php echo esc_attr( $int['name'] ); ?>"
								loading="lazy"
								onerror="this.style.display='none'"
							>
							<div class="p24-integration-card__type"><?php echo esc_html( $int['type'] ); ?></div>
							<h2 class="p24-h4"><?php echo esc_html( $int['name'] ); ?></h2>
							<p><?php echo $int['desc']; ?></p>
							<div class="p24-integration-card__features">
								<?php foreach ( $int['features'] as $feature ) : ?>
								<span class="p24-integration-card__feature-tag"><?php echo esc_html( $feature ); ?></span>
								<?php endforeach; ?>
							</div>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     OPEN API
			     ============================================================ -->

			<section class="p24-section p24-section-gray p24-integrations-api">
				<div class="p24-container">
					<div class="p24-integrations-api__box">
						<h2 class="p24-h3">Open API</h2>
						<p>
							PASS24 предоставляет открытый REST API для интеграции с&nbsp;любыми системами: ERP, WMS, CRM, умный дом и&nbsp;другие.
						</p>
						<code>POST /api/v1/passes — создать пропуск<br>GET /api/v1/passes/{id} — статус пропуска<br>GET /api/v1/journal — журнал проходов</code>
						<a href="/demo/" class="p24-btn p24-btn-primary">Запросить документацию API</a>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     CTA ПАРТНЁРСТВО
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container p24-integrations-partner">
					<h2 class="p24-h2" style="margin-bottom: 12px;">Стать партнёром по&nbsp;интеграции</h2>
					<p class="p24-subtitle" style="margin-bottom: 32px;">
						Если вы&nbsp;производитель оборудования СКУД или разработчик систем безопасности&nbsp;&mdash; давайте интегрируемся
					</p>
					<a href="/about/contacts/" class="p24-btn p24-btn-primary p24-btn-lg">Связаться с&nbsp;нами</a>
				</div>
			</section>


			<!-- ============================================================
			     ФИНАЛЬНЫЙ CTA
			     ============================================================ -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Работает с&nbsp;вашим оборудованием</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						Не&nbsp;нужно менять камеры, контроллеры и&nbsp;считыватели. PASS24 подключается к&nbsp;существующей инфраструктуре.
					</p>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот — 14 дней бесплатно</a>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
