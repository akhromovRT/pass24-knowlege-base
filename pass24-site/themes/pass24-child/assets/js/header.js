/**
 * PASS24 Header — мобильное меню и sticky-эффект
 *
 * @package PASS24_Child
 */

(function () {
	'use strict';

	var burger = document.getElementById('p24-burger');
	var menu = document.getElementById('p24-mobile-menu');
	var headerMain = document.getElementById('p24-header-main');

	if (!burger || !menu) return;

	/* ---- Mobile menu toggle ---- */

	function openMenu() {
		burger.classList.add('is-open');
		burger.setAttribute('aria-expanded', 'true');
		menu.classList.add('is-open');
		menu.setAttribute('aria-hidden', 'false');
		document.body.classList.add('p24-menu-open');
	}

	function closeMenu() {
		burger.classList.remove('is-open');
		burger.setAttribute('aria-expanded', 'false');
		menu.classList.remove('is-open');
		menu.setAttribute('aria-hidden', 'true');
		document.body.classList.remove('p24-menu-open');
	}

	burger.addEventListener('click', function () {
		var isOpen = burger.classList.contains('is-open');
		isOpen ? closeMenu() : openMenu();
	});

	// Close on Escape
	document.addEventListener('keydown', function (e) {
		if (e.key === 'Escape' && menu.classList.contains('is-open')) {
			closeMenu();
			burger.focus();
		}
	});

	// Close on nav link click
	var menuLinks = menu.querySelectorAll('.p24-mobile-menu__link');
	menuLinks.forEach(function (link) {
		link.addEventListener('click', closeMenu);
	});

	/* ---- Sticky header shadow ---- */

	if (headerMain) {
		var lastScroll = 0;
		window.addEventListener('scroll', function () {
			var scrollY = window.scrollY || window.pageYOffset;
			if (scrollY > 10) {
				headerMain.classList.add('is-scrolled');
			} else {
				headerMain.classList.remove('is-scrolled');
			}
			lastScroll = scrollY;
		}, { passive: true });
	}
})();
