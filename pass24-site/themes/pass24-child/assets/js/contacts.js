/**
 * PASS24 Contacts — форма обратной связи
 *
 * @package PASS24_Child
 */

(function () {
	'use strict';

	var form = document.getElementById('p24-contact-form-el');
	if (!form) return;

	var success = document.getElementById('p24-contacts-success');

	form.addEventListener('submit', function (e) {
		e.preventDefault();

		var name = form.querySelector('#p24c-name');
		var email = form.querySelector('#p24c-email');
		var message = form.querySelector('#p24c-message');

		// Basic validation
		if (!name.value.trim() || !email.value.trim() || !message.value.trim()) {
			return;
		}

		var btn = form.querySelector('.p24-contacts-submit');
		btn.classList.add('is-loading');
		btn.textContent = 'Отправка...';

		var data = {};
		new FormData(form).forEach(function (value, key) {
			if (key !== 'p24_contact_nonce' && key !== '_wp_http_referer') {
				data[key] = value;
			}
		});
		data.source = 'contact_form';
		data.page_url = window.location.href;

		var root = (window.wpApiSettings && window.wpApiSettings.root) || '/wp-json/';
		var nonce = (window.wpApiSettings && window.wpApiSettings.nonce) || '';

		fetch(root + 'pass24/v1/contact', {
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

		// GA4 via dataLayer
		if (window.dataLayer) {
			window.dataLayer.push({
				event: 'contact_form_submit',
				event_category: 'lead_generation',
				form_name: 'contact_form'
			});
			window.dataLayer.push({
				event: 'generate_lead',
				event_category: 'form_submission',
				form_name: 'contact_form'
			});
		}

		// Yandex.Metrika (counter ID hardcoded — Ya._metrika.counter is undocumented)
		if (window.ym) {
			window.ym(106989472, 'reachGoal', 'contact_form_submit');
			window.ym(106989472, 'reachGoal', 'generate_lead');
		}
	}
})();
