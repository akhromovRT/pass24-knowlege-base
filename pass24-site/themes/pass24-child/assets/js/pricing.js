/**
 * PASS24 Pricing Page
 *
 * 1. Переключатель помесячно / за год (скидка 10%)
 * 2. FAQ-аккордеон
 */

( function () {
	'use strict';

	/* ======================================================================
	   1. ПЕРЕКЛЮЧАТЕЛЬ ЦЕН
	   ======================================================================

	   HTML:
	   <div class="p24-pricing-toggle" data-p24-pricing-toggle>
	     <span class="p24-pricing-toggle__label is-active" data-period="monthly">Помесячно</span>
	     <button class="p24-pricing-toggle__switch" aria-label="Переключить период"></button>
	     <span class="p24-pricing-toggle__label" data-period="yearly">За год</span>
	     <span class="p24-pricing-toggle__save">-10%</span>
	   </div>

	   Цены в карточках:
	   <span class="p24-plan__price" data-price-monthly="10 990" data-price-yearly="9 891">
	     10 990
	   </span>
	   ====================================================================== */

	function initPricingToggle() {
		var container = document.querySelector( '[data-p24-pricing-toggle]' );
		if ( ! container ) return;

		var toggle = container.querySelector( '.p24-pricing-toggle__switch' );
		var labels = container.querySelectorAll( '.p24-pricing-toggle__label' );
		var prices = document.querySelectorAll( '[data-price-monthly]' );
		var yearlyHints = document.querySelectorAll( '.p24-plan__price-yearly' );
		var isYearly = false;

		function update() {
			toggle.classList.toggle( 'is-yearly', isYearly );

			labels.forEach( function ( label ) {
				var period = label.getAttribute( 'data-period' );
				label.classList.toggle( 'is-active',
					( isYearly && period === 'yearly' ) ||
					( ! isYearly && period === 'monthly' )
				);
			} );

			prices.forEach( function ( el ) {
				var value = isYearly
					? el.getAttribute( 'data-price-yearly' )
					: el.getAttribute( 'data-price-monthly' );
				el.textContent = value;
			} );

			yearlyHints.forEach( function ( el ) {
				el.style.visibility = isYearly ? 'hidden' : 'visible';
			} );
		}

		toggle.addEventListener( 'click', function () {
			isYearly = ! isYearly;
			update();
		} );

		labels.forEach( function ( label ) {
			label.addEventListener( 'click', function () {
				isYearly = this.getAttribute( 'data-period' ) === 'yearly';
				update();
			} );
		} );
	}

	/* ======================================================================
	   2. FAQ-АККОРДЕОН
	   ======================================================================

	   HTML:
	   <div class="p24-faq" data-p24-faq>
	     <div class="p24-faq__item">
	       <button class="p24-faq__question">
	         Вопрос?
	         <svg class="p24-faq__icon">...</svg>
	       </button>
	       <div class="p24-faq__answer">
	         <p>Ответ</p>
	       </div>
	     </div>
	   </div>
	   ====================================================================== */

	function initFaq() {
		var faqContainers = document.querySelectorAll( '[data-p24-faq]' );

		faqContainers.forEach( function ( faq ) {
			var items = faq.querySelectorAll( '.p24-faq__item' );

			items.forEach( function ( item ) {
				var question = item.querySelector( '.p24-faq__question' );

				question.addEventListener( 'click', function () {
					var wasOpen = item.classList.contains( 'is-open' );

					// Закрыть все
					items.forEach( function ( i ) {
						i.classList.remove( 'is-open' );
					} );

					// Открыть кликнутый (если не был открыт)
					if ( ! wasOpen ) {
						item.classList.add( 'is-open' );
					}
				} );
			} );
		} );
	}

	/* ======================================================================
	   Инициализация
	   ====================================================================== */

	document.addEventListener( 'DOMContentLoaded', function () {
		initPricingToggle();
		initFaq();

		// Track pricing page view
		if ( window.dataLayer ) {
			window.dataLayer.push( {
				event: 'pricing_viewed',
				event_category: 'engagement'
			} );
		}

		// Yandex.Metrika (counter ID hardcoded — Ya._metrika.counter is undocumented)
		if ( window.ym ) {
			window.ym( 106989472, 'reachGoal', 'pricing_viewed' );
		}
	} );
} )();
