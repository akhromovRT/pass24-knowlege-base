/**
 * PASS24 Homepage Components
 *
 * Интерактивные компоненты для главной страницы:
 * 1. Табы (секции 4, 5)
 * 2. Карусель кейсов (секция 8)
 * 3. Ротатор отзывов (секция 9)
 *
 * Карусель логотипов (секция 2) — чистый CSS, без JS.
 */

( function () {
	'use strict';

	/* ======================================================================
	   1. ТАБЫ — p24-tabs
	   ======================================================================

	   HTML-разметка для Bricks Builder:

	   <div class="p24-tabs" data-p24-tabs>
	     <div class="p24-tabs__nav" role="tablist">
	       <button class="p24-tabs__trigger is-active" data-tab="0" role="tab"
	               aria-selected="true" aria-controls="tab-panel-0">
	         Бюро пропусков
	       </button>
	       <button class="p24-tabs__trigger" data-tab="1" role="tab"
	               aria-selected="false" aria-controls="tab-panel-1">
	         Распознавание номеров
	       </button>
	       ...
	     </div>
	     <div class="p24-tabs__panels">
	       <div class="p24-tabs__panel is-active" id="tab-panel-0" role="tabpanel">
	         Контент первой вкладки
	       </div>
	       <div class="p24-tabs__panel" id="tab-panel-1" role="tabpanel">
	         Контент второй вкладки
	       </div>
	       ...
	     </div>
	   </div>
	   ====================================================================== */

	function initTabs() {
		var tabGroups = document.querySelectorAll( '[data-p24-tabs]' );

		tabGroups.forEach( function ( group ) {
			var triggers = group.querySelectorAll( '.p24-tabs__trigger' );
			var panels = group.querySelectorAll( '.p24-tabs__panel' );

			triggers.forEach( function ( trigger ) {
				trigger.addEventListener( 'click', function () {
					var index = parseInt( this.getAttribute( 'data-tab' ), 10 );

					// Деактивировать все
					triggers.forEach( function ( t ) {
						t.classList.remove( 'is-active' );
						t.setAttribute( 'aria-selected', 'false' );
					} );
					panels.forEach( function ( p ) {
						p.classList.remove( 'is-active' );
					} );

					// Активировать выбранный
					this.classList.add( 'is-active' );
					this.setAttribute( 'aria-selected', 'true' );
					if ( panels[ index ] ) {
						panels[ index ].classList.add( 'is-active' );
					}
				} );

				// Keyboard: стрелки влево/вправо для переключения
				trigger.addEventListener( 'keydown', function ( e ) {
					var index = parseInt( this.getAttribute( 'data-tab' ), 10 );
					var next;

					if ( e.key === 'ArrowRight' ) {
						next = triggers[ ( index + 1 ) % triggers.length ];
					} else if ( e.key === 'ArrowLeft' ) {
						next = triggers[ ( index - 1 + triggers.length ) % triggers.length ];
					}

					if ( next ) {
						e.preventDefault();
						next.focus();
						next.click();
					}
				} );
			} );
		} );
	}

	/* ======================================================================
	   2. КАРУСЕЛЬ КЕЙСОВ — p24-carousel
	   ======================================================================

	   HTML-разметка:

	   <div class="p24-carousel" data-p24-carousel>
	     <div class="p24-carousel__track">
	       <div class="p24-carousel__slide">Кейс 1</div>
	       <div class="p24-carousel__slide">Кейс 2</div>
	       <div class="p24-carousel__slide">Кейс 3</div>
	       <div class="p24-carousel__slide">Кейс 4</div>
	     </div>
	     <div class="p24-carousel__controls">
	       <button class="p24-carousel__prev" aria-label="Предыдущий">&larr;</button>
	       <div class="p24-carousel__dots"></div>
	       <button class="p24-carousel__next" aria-label="Следующий">&rarr;</button>
	     </div>
	   </div>
	   ====================================================================== */

	function initCarousels() {
		var carousels = document.querySelectorAll( '[data-p24-carousel]' );

		carousels.forEach( function ( container ) {
			var track = container.querySelector( '.p24-carousel__track' );
			var slides = container.querySelectorAll( '.p24-carousel__slide' );
			var prevBtn = container.querySelector( '.p24-carousel__prev' );
			var nextBtn = container.querySelector( '.p24-carousel__next' );
			var dotsContainer = container.querySelector( '.p24-carousel__dots' );

			if ( ! track || slides.length === 0 ) return;

			var current = 0;
			var total = slides.length;

			// Создать точки навигации
			if ( dotsContainer ) {
				for ( var i = 0; i < total; i++ ) {
					var dot = document.createElement( 'button' );
					dot.className = 'p24-carousel__dot' + ( i === 0 ? ' is-active' : '' );
					dot.setAttribute( 'aria-label', 'Слайд ' + ( i + 1 ) );
					dot.setAttribute( 'data-slide', i );
					dotsContainer.appendChild( dot );
				}

				dotsContainer.addEventListener( 'click', function ( e ) {
					var dot = e.target.closest( '.p24-carousel__dot' );
					if ( dot ) {
						goTo( parseInt( dot.getAttribute( 'data-slide' ), 10 ) );
					}
				} );
			}

			function goTo( index ) {
				current = ( index + total ) % total;
				track.style.transform = 'translateX(-' + ( current * 100 ) + '%)';
				updateDots();
			}

			function updateDots() {
				if ( ! dotsContainer ) return;
				var dots = dotsContainer.querySelectorAll( '.p24-carousel__dot' );
				dots.forEach( function ( dot, i ) {
					dot.classList.toggle( 'is-active', i === current );
				} );
			}

			if ( prevBtn ) {
				prevBtn.addEventListener( 'click', function () {
					goTo( current - 1 );
				} );
			}

			if ( nextBtn ) {
				nextBtn.addEventListener( 'click', function () {
					goTo( current + 1 );
				} );
			}

			// Touch/swipe
			var startX = 0;
			var isDragging = false;

			track.addEventListener( 'touchstart', function ( e ) {
				startX = e.touches[ 0 ].clientX;
				isDragging = true;
			}, { passive: true } );

			track.addEventListener( 'touchend', function ( e ) {
				if ( ! isDragging ) return;
				isDragging = false;
				var diff = startX - e.changedTouches[ 0 ].clientX;
				if ( Math.abs( diff ) > 50 ) {
					goTo( diff > 0 ? current + 1 : current - 1 );
				}
			}, { passive: true } );
		} );
	}

	/* ======================================================================
	   3. РОТАТОР ОТЗЫВОВ — p24-testimonials
	   ======================================================================

	   HTML-разметка:

	   <div class="p24-testimonials" data-p24-testimonials>
	     <div class="p24-testimonials__item is-active">
	       <blockquote class="p24-testimonials__quote">
	         «Текст отзыва с <strong>ключевыми фразами</strong>»
	       </blockquote>
	       <div class="p24-testimonials__author">
	         <img src="photo.jpg" alt="" class="p24-testimonials__photo">
	         <div>
	           <div class="p24-testimonials__name">Имя Фамилия</div>
	           <div class="p24-testimonials__role">Должность, Компания</div>
	         </div>
	       </div>
	     </div>
	     <div class="p24-testimonials__item">...</div>
	     <div class="p24-testimonials__item">...</div>
	   </div>
	   ====================================================================== */

	function initTestimonials() {
		var containers = document.querySelectorAll( '[data-p24-testimonials]' );

		containers.forEach( function ( container ) {
			var items = container.querySelectorAll( '.p24-testimonials__item' );
			if ( items.length < 2 ) return;

			var current = 0;
			var total = items.length;
			var interval = null;
			var DELAY = 6000; // 6 секунд на отзыв

			function showNext() {
				items[ current ].classList.remove( 'is-active' );
				current = ( current + 1 ) % total;
				items[ current ].classList.add( 'is-active' );
			}

			function startAutoplay() {
				interval = setInterval( showNext, DELAY );
			}

			function stopAutoplay() {
				clearInterval( interval );
			}

			// Пауза при наведении
			container.addEventListener( 'mouseenter', stopAutoplay );
			container.addEventListener( 'mouseleave', startAutoplay );

			startAutoplay();
		} );
	}

	/* ======================================================================
	   Инициализация
	   ====================================================================== */

	document.addEventListener( 'DOMContentLoaded', function () {
		initTabs();
		initCarousels();
		initTestimonials();
	} );
} )();
