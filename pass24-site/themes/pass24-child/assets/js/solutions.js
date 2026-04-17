/**
 * PASS24 Solutions — CTA-форма с auto-select сегмента
 *
 * @package PASS24_Child
 */

(function () {
	'use strict';

	// Auto-select segment in dropdown
	var segmentField = document.querySelector('[data-p24-segment-field]');
	if (segmentField) {
		var autoValue = segmentField.getAttribute('data-p24-auto-segment');
		if (autoValue) {
			segmentField.value = autoValue;
		}
	}

	// Form submission
	var form = document.getElementById('p24-solution-form-el');
	if (!form) return;

	var success = document.getElementById('p24-solution-success');

	form.addEventListener('submit', function (e) {
		e.preventDefault();

		var name = form.querySelector('#p24s-name');
		var phone = form.querySelector('#p24s-phone');

		// Basic validation
		if (!name.value.trim() || !phone.value.trim()) {
			return;
		}

		var btn = form.querySelector('.p24-contacts-submit');
		btn.classList.add('is-loading');
		btn.textContent = 'Отправка...';

		var data = {};
		new FormData(form).forEach(function (value, key) {
			if (key !== 'p24_solution_nonce' && key !== '_wp_http_referer') {
				data[key] = value;
			}
		});
		data.source = 'solution_form';
		data.page_url = window.location.href;

		var root = (window.wpApiSettings && window.wpApiSettings.root) || '/wp-json/';
		var nonce = (window.wpApiSettings && window.wpApiSettings.nonce) || '';

		fetch(root + 'pass24/v1/demo-request', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'X-WP-Nonce': nonce
			},
			body: JSON.stringify(data)
		})
		.then(function () { showSuccess(); })
		.catch(function () { showSuccess(); });
	});

	function showSuccess() {
		form.style.display = 'none';
		if (success) success.style.display = 'block';

		// GA4
		if (window.dataLayer) {
			window.dataLayer.push({
				event: 'solution_form_submit',
				event_category: 'lead_generation',
				form_name: 'solution_form',
				segment: segmentField ? segmentField.value : ''
			});
		}
	}
})();
