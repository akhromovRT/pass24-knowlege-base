<?php
/**
 * Template Part: Product Page (7 секций)
 *
 * Переменная $product должна быть определена перед include.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

if ( empty( $product ) ) {
	return;
}

// Подключаем данные интеграций для секции 4.
require_once PASS24_CHILD_DIR . '/inc/integration-data.php';
$all_integrations = pass24_get_integrations();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- ============================================================
			     1. HERO
			     ============================================================ -->

			<section class="p24-section p24-product-hero">
				<div class="p24-container">
					<div class="p24-product-hero__layout">
						<div class="p24-product-hero__content">
							<span class="p24-badge p24-badge-accent"><?php echo $product['badge']; ?></span>
							<h1 class="p24-h1"><?php echo $product['hero_title']; ?></h1>
							<p class="p24-subtitle"><?php echo $product['hero_subtitle']; ?></p>
							<div class="p24-product-hero__actions">
								<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Начать пилот — 14 дней</a>
								<a href="/demo/" class="p24-btn p24-btn-secondary p24-btn-lg">Посмотреть демо</a>
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


			<!-- ============================================================
			     2. КАК ЭТО РАБОТАЕТ (3 шага)
			     ============================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Как это работает</h2>
						<p class="p24-subtitle">Три простых шага для начала работы</p>
					</div>
					<div class="p24-product-steps">
						<?php foreach ( $product['steps'] as $step ) : ?>
						<div class="p24-product-step">
							<div class="p24-product-step__num"><?php echo esc_html( $step['num'] ); ?></div>
							<h3 class="p24-h4"><?php echo $step['title']; ?></h3>
							<p><?php echo $step['desc']; ?></p>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     3. КЛЮЧЕВЫЕ ВОЗМОЖНОСТИ
			     ============================================================ -->

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


			<!-- ============================================================
			     4. ИНТЕГРАЦИИ
			     ============================================================ -->

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
					<div style="text-align: center; margin-top: 32px;">
						<a href="/integrations/" class="p24-btn p24-btn-secondary">Все интеграции</a>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     5. ПРИМЕНЕНИЕ ПО СЕГМЕНТАМ
			     ============================================================ -->

			<?php if ( ! empty( $product['use_cases'] ) ) : ?>
			<section class="p24-section">
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
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     6. КЕЙС
			     ============================================================ -->

			<?php if ( ! empty( $product['case_study']['client'] ) ) : ?>
			<section class="p24-section p24-section-gray">
				<div class="p24-container" style="max-width: 800px;">
					<div class="p24-product-case">
						<span class="p24-badge p24-badge-primary"><?php echo esc_html( $product['case_study']['segment'] ); ?></span>
						<h2 class="p24-h3" style="margin-top: 12px;"><?php echo esc_html( $product['case_study']['client'] ); ?></h2>
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


			<!-- ============================================================
			     7. ФИНАЛЬНЫЙ CTA
			     ============================================================ -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Попробуйте <?php echo esc_html( $product['name'] ); ?></h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						14 дней бесплатно. Обучение и&nbsp;онлайн&#8209;сопровождение включены.
					</p>
					<div style="display: flex; gap: 16px; justify-content: center; flex-wrap: wrap;">
						<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот — 14 дней</a>
						<a href="/pricing/" class="p24-btn p24-btn-ghost p24-btn-lg">Тарифы</a>
					</div>
					<ul class="p24-cta-checklist" style="justify-content: center; margin-top: 24px; color: rgba(255,255,255,.6);">
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
