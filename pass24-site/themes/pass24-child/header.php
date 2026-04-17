<?php
/**
 * PASS24 Child Theme — header.php
 *
 * Кастомная шапка: top bar (телефон + вход), основной header (логотип, навигация, CTA),
 * мобильное бургер-меню. Sticky при скролле.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;
?>
<!DOCTYPE html>
<html <?php language_attributes(); ?>>
<head>
	<meta charset="<?php bloginfo( 'charset' ); ?>">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<?php wp_head(); ?>
</head>

<body <?php body_class(); ?>>
<?php
if ( function_exists( 'wp_body_open' ) ) {
	wp_body_open();
}

do_action( 'generate_before_header' );
?>

<header class="p24-header" id="p24-header" itemscope itemtype="https://schema.org/WPHeader">

	<!-- Top bar -->
	<div class="p24-header__topbar">
		<div class="p24-container p24-header__topbar-inner">
			<a href="tel:+74954144455" class="p24-header__phone">
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07 19.5 19.5 0 01-6-6 19.79 19.79 0 01-3.07-8.67A2 2 0 014.11 2h3a2 2 0 012 1.72c.127.96.361 1.903.7 2.81a2 2 0 01-.45 2.11L8.09 9.91a16 16 0 006 6l1.27-1.27a2 2 0 012.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0122 16.92z"/></svg>
				+7 (495) 414-44-55
			</a>
			<a href="https://admin.pass24.online" class="p24-header__login" target="_blank" rel="noopener noreferrer">
				Войти в PASS24.admin
				<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
			</a>
		</div>
	</div>

	<!-- Main header -->
	<div class="p24-header__main" id="p24-header-main">
		<div class="p24-container p24-header__main-inner">

			<!-- Logo -->
			<a href="<?php echo esc_url( home_url( '/' ) ); ?>" class="p24-header__logo" aria-label="PASS24.online — главная">
				PASS24<span class="p24-header__logo-dot">.online</span>
			</a>

			<!-- Navigation -->
			<nav class="p24-header__nav" id="p24-nav" aria-label="Основная навигация">
				<?php
				$nav_items = [
					'/products/'   => 'Продукты',
					'/solutions/'  => 'Решения',
					'/pricing/'    => 'Тарифы',
					'/blog/'       => 'Блог',
					'/about/'      => 'О нас',
				];
				foreach ( $nav_items as $url => $label ) :
					$is_active = ( is_page( basename( rtrim( $url, '/' ) ) ) ) ? ' is-active' : '';
				?>
					<a href="<?php echo esc_url( $url ); ?>" class="p24-header__nav-link<?php echo $is_active; ?>">
						<?php echo esc_html( $label ); ?>
					</a>
				<?php endforeach; ?>
			</nav>

			<!-- CTA -->
			<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-sm p24-header__cta">
				Попробовать бесплатно
			</a>

			<!-- Burger button (mobile) -->
			<button class="p24-header__burger" id="p24-burger" aria-label="Открыть меню" aria-expanded="false" aria-controls="p24-mobile-menu">
				<span></span>
				<span></span>
				<span></span>
			</button>

		</div>
	</div>

	<!-- Mobile menu overlay -->
	<div class="p24-mobile-menu" id="p24-mobile-menu" aria-hidden="true">
		<nav class="p24-mobile-menu__nav" aria-label="Мобильная навигация">
			<?php foreach ( $nav_items as $url => $label ) : ?>
				<a href="<?php echo esc_url( $url ); ?>" class="p24-mobile-menu__link">
					<?php echo esc_html( $label ); ?>
				</a>
			<?php endforeach; ?>
		</nav>
		<div class="p24-mobile-menu__footer">
			<a href="/demo/" class="p24-btn p24-btn-primary" style="width: 100%; justify-content: center;">
				Попробовать бесплатно — 14 дней
			</a>
			<a href="tel:+74954144455" class="p24-mobile-menu__phone">
				+7 (495) 414-44-55
			</a>
			<a href="https://admin.pass24.online" class="p24-mobile-menu__login" target="_blank" rel="noopener noreferrer">
				Войти в PASS24.admin &rarr;
			</a>
		</div>
	</div>

</header>

<?php do_action( 'generate_after_header' ); ?>

<div class="site grid-container">
