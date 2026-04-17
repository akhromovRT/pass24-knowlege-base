<?php
/**
 * Template: Cases Hub / Кейсы
 *
 * Хаб-страница /cases/ — карточная сетка всех кейсов.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/case-data.php';
$cases = pass24_get_cases();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- HERO -->

			<section class="p24-case-hero">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<h1 class="p24-h1" style="margin-bottom: 16px;">Кейсы клиентов</h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
						Реальные истории внедрения PASS24 на&nbsp;объектах разных типов&nbsp;&mdash; от&nbsp;элитных ЖК до&nbsp;логистических комплексов
					</p>
				</div>
			</section>


			<!-- CASES GRID -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-cases-grid">
						<?php foreach ( $cases as $slug => $c ) : ?>
						<a href="/cases/<?php echo esc_attr( $slug ); ?>/" class="p24-card p24-case-hub-card">
							<div class="p24-case-hub-card__segment"><?php echo esc_html( $c['segment'] ); ?></div>
							<h2 class="p24-h4"><?php echo esc_html( $c['name'] ); ?></h2>
							<p style="color: var(--p24-text-secondary); margin: 8px 0 0;">
								<?php echo wp_strip_all_tags( $c['hero_quote'] ); ?>
							</p>
							<?php if ( ! empty( $c['metrics'][0] ) ) : ?>
							<span class="p24-case-hub-card__metric"><?php echo $c['metrics'][0]['value']; ?> <?php echo wp_strip_all_tags( $c['metrics'][0]['label'] ); ?></span>
							<?php endif; ?>
							<span class="p24-case-hub-card__link">Читать кейс &rarr;</span>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- CTA -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Ваш объект&nbsp;&mdash; следующий кейс</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						Запустите пилот PASS24 и&nbsp;получите результаты за&nbsp;14&nbsp;дней.
					</p>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот &mdash; 14 дней бесплатно</a>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
