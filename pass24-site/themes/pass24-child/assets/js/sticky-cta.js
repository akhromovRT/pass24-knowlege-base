/**
 * PASS24 Sticky CTA Bar
 *
 * Показывает фиксированную полосу с CTA после прокрутки вниз на 600px.
 * Скрывает, когда пользователь возвращается к верху страницы.
 *
 * HTML-разметка (добавить в Bricks Builder → Footer или шаблон):
 *
 * <div class="p24-sticky-cta" id="p24-sticky-cta">
 *   <a href="/demo/" class="p24-btn p24-btn-primary p24-btn-sm">
 *     Попробовать бесплатно — 14 дней
 *   </a>
 *   <span class="p24-sticky-cta__divider"></span>
 *   <a href="tel:+74954144455" class="p24-sticky-cta__phone">
 *     +7 (495) 123-45-67
 *   </a>
 * </div>
 */

( function () {
	'use strict';

	var bar = document.getElementById( 'p24-sticky-cta' );
	if ( ! bar ) return;

	var threshold = 600;
	var visible = false;

	function onScroll() {
		var shouldShow = window.scrollY > threshold;
		if ( shouldShow === visible ) return;

		visible = shouldShow;
		if ( shouldShow ) {
			bar.classList.add( 'is-visible' );
		} else {
			bar.classList.remove( 'is-visible' );
		}
	}

	window.addEventListener( 'scroll', onScroll, { passive: true } );
} )();
