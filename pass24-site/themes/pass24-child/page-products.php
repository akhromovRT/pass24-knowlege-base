<?php
/**
 * Template: Products Hub / Каталог продуктов
 *
 * Хаб-страница /products/ — карточная сетка всех продуктов PASS24.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/product-data.php';
$products = pass24_get_products();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- HERO -->

			<section class="p24-products-hub-hero">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<h1 class="p24-h1" style="margin-bottom: 16px;">Продукты PASS24</h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
						6 продуктов для полного контроля доступа&nbsp;&mdash; от&nbsp;мобильных пропусков до&nbsp;облачной СКУД
					</p>
				</div>
			</section>


			<!-- PRODUCTS GRID -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-products-grid">
						<?php foreach ( $products as $slug => $prod ) : ?>
						<a href="/products/<?php echo esc_attr( $slug ); ?>/" class="p24-card p24-product-hub-card">
							<span class="p24-badge p24-badge-accent"><?php echo $prod['badge']; ?></span>
							<h2 class="p24-h4"><?php echo esc_html( $prod['name'] ); ?></h2>
							<p><?php echo wp_strip_all_tags( $prod['hero_subtitle'] ); ?></p>
							<span class="p24-product-hub-card__link">Подробнее &rarr;</span>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- CTA -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Не&nbsp;знаете, что выбрать?</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						Запросите демонстрацию&nbsp;&mdash; мы&nbsp;подберём оптимальную конфигурацию для вашего объекта
					</p>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Запросить демо — 15 минут</a>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
