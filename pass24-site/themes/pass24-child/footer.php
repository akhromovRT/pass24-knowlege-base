<?php
/**
 * PASS24 Child Theme — footer.php
 *
 * Кастомный footer: навигация, контакты, соцсети, юридическая информация.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

require_once PASS24_CHILD_DIR . '/inc/product-data.php';
require_once PASS24_CHILD_DIR . '/inc/solution-data.php';

$footer_products  = pass24_get_products();
$footer_solutions = array_slice( pass24_get_solutions(), 0, 5, true );

// Закрывающие теги </main>, </div>, </div> уже в шаблонах (front-page.php, page-pricing.php).
// footer.php закрывает .site.grid-container (из header.php) и выводит <footer>.
?>

</div><!-- .site.grid-container (opened in header.php) -->

<?php do_action( 'generate_before_footer' ); ?>

<footer class="p24-footer" itemscope itemtype="https://schema.org/WPFooter">

	<!-- Основной блок footer -->
	<div class="p24-footer__main">
		<div class="p24-container">
			<div class="p24-footer__grid">

				<!-- Колонка 1: О компании -->
				<div class="p24-footer__col p24-footer__col--about">
					<a href="<?php echo esc_url( home_url( '/' ) ); ?>" class="p24-footer__logo">
						PASS24.online
					</a>
					<p class="p24-footer__desc">
						Облачная система контроля и управления доступом. 300+ объектов по всей России.
					</p>
					<div class="p24-footer__socials">
						<a href="https://t.me/pass24" target="_blank" rel="noopener noreferrer" aria-label="Telegram">
							<svg width="24" height="24" viewBox="0 0 24 24" fill="none"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z" fill="currentColor"/></svg>
						</a>
						<a href="https://vk.com/pass24" target="_blank" rel="noopener noreferrer" aria-label="ВКонтакте">
							<svg width="24" height="24" viewBox="0 0 24 24" fill="none"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm5.01 13.49h-1.3c-.49 0-.64-.39-1.52-1.28-.77-.74-1.1-.84-1.29-.84-.26 0-.34.07-.34.43v1.17c0 .31-.1.49-1.09.49-1.6 0-3.37-.97-4.62-2.79C5.42 10.56 5 8.73 5 8.41c0-.19.07-.36.43-.36h1.3c.32 0 .44.14.56.49.62 1.79 1.66 3.36 2.09 3.36.16 0 .23-.07.23-.48V9.85c-.05-.83-.49-.9-.49-1.2 0-.15.12-.3.32-.3h2.04c.27 0 .36.14.36.46v2.58c0 .27.12.36.2.36.16 0 .29-.09.58-.39 1.41-1.28 1.89-3.37 1.89-3.37.1-.19.27-.36.58-.36h1.3c.39 0 .48.2.39.46-.17.92-1.82 3.13-1.82 3.13-.15.24-.2.34 0 .61.14.19.63.61.95 1 .66.7 1.16 1.29 1.29 1.7.14.41-.07.62-.46.62z" fill="currentColor"/></svg>
						</a>
					</div>
				</div>

				<!-- Колонка 2: Продукты (динамически из product-data.php) -->
				<div class="p24-footer__col">
					<h4 class="p24-footer__heading">Продукты</h4>
					<ul class="p24-footer__links">
						<?php foreach ( $footer_products as $slug => $prod ) : ?>
						<li><a href="/products/<?php echo esc_attr( $slug ); ?>/"><?php echo esc_html( $prod['name'] ); ?></a></li>
						<?php endforeach; ?>
					</ul>
				</div>

				<!-- Колонка 3: Решения (динамически из solution-data.php, топ-5) -->
				<div class="p24-footer__col">
					<h4 class="p24-footer__heading">Решения</h4>
					<ul class="p24-footer__links">
						<?php foreach ( $footer_solutions as $slug => $sol ) : ?>
						<li><a href="/solutions/<?php echo esc_attr( $slug ); ?>/"><?php echo esc_html( $sol['nav_name'] ); ?></a></li>
						<?php endforeach; ?>
					</ul>
				</div>

				<!-- Колонка 4: Компания -->
				<div class="p24-footer__col">
					<h4 class="p24-footer__heading">Компания</h4>
					<ul class="p24-footer__links">
						<li><a href="/about/">О компании</a></li>
						<li><a href="/blog/">Блог</a></li>
						<li><a href="/pricing/">Тарифы</a></li>
						<li><a href="/integrations/">Интеграции</a></li>
						<li><a href="/demo/">Запросить демо</a></li>
					</ul>
				</div>

				<!-- Колонка 5: Контакты -->
				<div class="p24-footer__col">
					<h4 class="p24-footer__heading">Контакты</h4>
					<ul class="p24-footer__links p24-footer__contacts">
						<li>
							<a href="tel:+74954144455">+7 (495) 414-44-55</a>
						</li>
						<li>
							<a href="mailto:info@pass24online.ru">info@pass24online.ru</a>
						</li>
						<li>
							Москва, Россия
						</li>
					</ul>
					<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-sm p24-footer__cta">
						Запросить демо
					</a>
				</div>

			</div>
		</div>
	</div>

	<!-- Нижняя полоса -->
	<div class="p24-footer__bottom">
		<div class="p24-container">
			<div class="p24-footer__bottom-inner">
				<div class="p24-footer__copy">
					&copy; <?php echo date( 'Y' ); ?> PASS24.online. Все права защищены.
				</div>
				<div class="p24-footer__legal">
					<a href="/privacy/">Политика конфиденциальности</a>
					<a href="/terms/">Условия использования</a>
					<span class="p24-footer__registry">Реестр российского ПО</span>
				</div>
			</div>
		</div>
	</div>

</footer>

<?php do_action( 'generate_after_footer' ); ?>

<!-- Sticky CTA-бар (глобальный, все страницы) -->
<div class="p24-sticky-cta" id="p24-sticky-cta">
	<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-sm">
		Попробовать бесплатно — 14 дней
	</a>
	<span class="p24-sticky-cta__divider"></span>
	<a href="tel:+74954144455" class="p24-sticky-cta__phone">
		+7 (495) 414-44-55
	</a>
</div>

<?php wp_footer(); ?>

</body>
</html>
