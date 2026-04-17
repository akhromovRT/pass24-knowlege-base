<?php
/**
 * Shared Case Study Template
 *
 * 7 секций: Hero → Задача → Решение → Результаты → Отзыв → Продукты → CTA
 * Получает переменную $case из dispatcher.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/product-data.php';

$all_products = pass24_get_products();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- ============================================================
			     HERO
			     ============================================================ -->

			<section class="p24-case-hero">
				<div class="p24-container" style="max-width: 800px; text-align: center;">
					<span class="p24-badge p24-badge-dark"><?php echo esc_html( $case['segment'] ); ?></span>
					<h1 class="p24-h1" style="margin: 16px 0 12px;"><?php echo esc_html( $case['name'] ); ?></h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto 24px;">
						<?php echo $case['hero_quote']; ?>
					</p>
					<?php if ( ! empty( $case['timeline'] ) ) : ?>
					<span class="p24-small" style="color: var(--p24-text-secondary);">Внедрение: <?php echo esc_html( $case['timeline'] ); ?></span>
					<?php endif; ?>
				</div>
			</section>


			<!-- ============================================================
			     ЗАДАЧА
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Задача</h2>
						<p class="p24-subtitle">С&nbsp;какими проблемами столкнулся клиент</p>
					</div>
					<div class="p24-grid-<?php echo count( $case['challenge'] ) <= 3 ? '3' : '2'; ?>" style="margin-top: 32px;">
						<?php foreach ( $case['challenge'] as $item ) : ?>
						<div class="p24-card" style="padding: 24px;">
							<h3 class="p24-h4" style="margin-bottom: 8px;"><?php echo $item['title']; ?></h3>
							<p style="color: var(--p24-text-secondary); margin: 0;"><?php echo $item['desc']; ?></p>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     РЕШЕНИЕ
			     ============================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Решение</h2>
						<p class="p24-subtitle">Что мы&nbsp;внедрили</p>
					</div>
					<div class="p24-grid-<?php echo count( $case['solution'] ) <= 3 ? '3' : '2'; ?>" style="margin-top: 32px;">
						<?php foreach ( $case['solution'] as $item ) : ?>
						<div class="p24-card" style="padding: 24px;">
							<h3 class="p24-h4" style="margin-bottom: 8px; color: var(--p24-primary);"><?php echo $item['title']; ?></h3>
							<p style="color: var(--p24-text-secondary); margin: 0;"><?php echo $item['desc']; ?></p>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     РЕЗУЛЬТАТЫ
			     ============================================================ -->

			<?php if ( ! empty( $case['metrics'] ) ) : ?>
			<section class="p24-section p24-section-dark">
				<div class="p24-container">
					<h2 class="p24-h2" style="text-align: center; color: #fff; margin-bottom: 40px;">Результаты внедрения</h2>
					<div class="p24-case-metrics">
						<?php foreach ( $case['metrics'] as $m ) : ?>
						<div class="p24-case-metrics__item">
							<div class="p24-case-metrics__value"><?php echo $m['value']; ?></div>
							<div class="p24-case-metrics__label"><?php echo $m['label']; ?></div>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     ОТЗЫВ
			     ============================================================ -->

			<?php if ( ! empty( $case['testimonial'] ) ) : ?>
			<section class="p24-section">
				<div class="p24-container" style="max-width: 700px;">
					<blockquote class="p24-case-testimonial">
						<p class="p24-case-testimonial__text">&laquo;<?php echo $case['testimonial']['quote']; ?>&raquo;</p>
						<footer class="p24-case-testimonial__author">
							<strong><?php echo esc_html( $case['testimonial']['name'] ); ?></strong>
							<span><?php echo esc_html( $case['testimonial']['role'] ); ?></span>
						</footer>
					</blockquote>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     ИСПОЛЬЗОВАННЫЕ ПРОДУКТЫ
			     ============================================================ -->

			<?php if ( ! empty( $case['products'] ) ) : ?>
			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Использованные продукты</h2>
					</div>
					<div class="p24-grid-2" style="margin-top: 32px;">
						<?php foreach ( $case['products'] as $p_slug ) :
							if ( empty( $all_products[ $p_slug ] ) ) continue;
							$p = $all_products[ $p_slug ];
						?>
						<a href="/products/<?php echo esc_attr( $p_slug ); ?>/" class="p24-card p24-case-product-card">
							<span class="p24-badge p24-badge-primary"><?php echo esc_html( $p['badge'] ); ?></span>
							<h3 class="p24-h4" style="margin: 8px 0 4px;"><?php echo esc_html( $p['name'] ); ?></h3>
							<p style="color: var(--p24-text-secondary); margin: 0;"><?php echo wp_strip_all_tags( $p['hero_subtitle'] ); ?></p>
							<span class="p24-case-product-card__link">Подробнее &rarr;</span>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     CTA
			     ============================================================ -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Хотите таких&nbsp;же результатов?</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						Запустите пилот PASS24 на&nbsp;вашем объекте&nbsp;&mdash; 14&nbsp;дней бесплатно, обучение включено.
					</p>
					<div style="display: flex; gap: 12px; justify-content: center; flex-wrap: wrap;">
						<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот &mdash; 14 дней</a>
						<?php if ( ! empty( $case['solution_page'] ) ) : ?>
						<a href="<?php echo esc_url( $case['solution_page'] ); ?>" class="p24-btn p24-btn-ghost p24-btn-lg" style="color: #fff; border-color: rgba(255,255,255,.3);">Решение для <?php echo esc_html( $case['segment'] ); ?></a>
						<?php endif; ?>
					</div>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
