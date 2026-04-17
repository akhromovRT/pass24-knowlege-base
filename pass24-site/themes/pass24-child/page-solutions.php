<?php
/**
 * Template: Solutions Hub / Решения по сегментам
 *
 * Хаб-страница /solutions/ — карточная сетка всех вертикалей.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/solution-data.php';
$solutions = pass24_get_solutions();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- HERO -->

			<section class="p24-solutions-hub-hero">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<h1 class="p24-h1" style="margin-bottom: 16px;">Решения по&nbsp;типу объекта</h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
						PASS24 адаптируется под специфику каждого сегмента&nbsp;&mdash; от&nbsp;жилых комплексов до&nbsp;промышленных объектов
					</p>
				</div>
			</section>


			<!-- SOLUTIONS GRID -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-solutions-grid">
						<?php foreach ( $solutions as $slug => $sol ) : ?>
						<a href="/solutions/<?php echo esc_attr( $slug ); ?>/" class="p24-card p24-solution-hub-card">
							<div class="p24-solution-hub-card__priority"><?php echo esc_html( $sol['priority'] ); ?></div>
							<h2 class="p24-h4"><?php echo esc_html( $sol['name'] ); ?></h2>
							<p><?php echo wp_strip_all_tags( $sol['hero_subtitle'] ); ?></p>
							<?php if ( ! empty( $sol['clients'] ) ) : ?>
							<div class="p24-solution-hub-card__clients">
								Клиенты: <?php echo esc_html( implode( ', ', $sol['clients'] ) ); ?>
							</div>
							<?php endif; ?>
							<span class="p24-solution-hub-card__link">Подробнее &rarr;</span>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- CTA -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Не&nbsp;нашли свой тип объекта?</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						PASS24 подходит для любых объектов с&nbsp;контролем доступа. Расскажите о&nbsp;вашей задаче.
					</p>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Обсудить решение</a>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
