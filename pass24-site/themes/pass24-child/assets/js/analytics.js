/**
 * PASS24 Global Analytics — UTM cookie setter
 *
 * Загружается на ВСЕХ страницах. Парсит UTM-параметры из URL,
 * сохраняет в cookies на 30 дней для последующей передачи в формы.
 *
 * @package PASS24_Child
 */
(function () {
	'use strict';

	var UTM_PARAMS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content'];
	var COOKIE_DAYS = 30;

	function getUrlParam(name) {
		var params = new URLSearchParams(window.location.search);
		return params.get(name);
	}

	function setCookie(name, value, days) {
		var date = new Date();
		date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
		document.cookie = name + '=' + encodeURIComponent(value) +
			'; expires=' + date.toUTCString() +
			'; path=/; SameSite=Lax';
	}

	// Save UTM params from URL to cookies (30 days)
	var hasUtm = false;
	UTM_PARAMS.forEach(function (param) {
		var value = getUrlParam(param);
		if (value) {
			setCookie(param, value, COOKIE_DAYS);
			hasUtm = true;
		}
	});

	if (hasUtm) {
		setCookie('utm_landing', encodeURIComponent(window.location.pathname), COOKIE_DAYS);
	}
})();
