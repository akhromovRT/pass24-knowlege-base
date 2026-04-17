<?php
/**
 * Template: Contacts / Контакты
 *
 * Страница контактов: адрес, телефон, email, мессенджеры, форма обратной связи, карта.
 * Используется для /about/contacts/ (slug: contacts).
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- ============================================================
			     HERO
			     ============================================================ -->

			<section class="p24-contacts-hero">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<h1 class="p24-h1" style="margin-bottom: 16px;">Свяжитесь с&nbsp;нами</h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
						Ответим на&nbsp;вопросы, подберём решение под ваш объект и&nbsp;организуем демонстрацию
					</p>
				</div>
			</section>


			<!-- ============================================================
			     КОНТАКТЫ + ФОРМА
			     ============================================================ -->

			<section class="p24-section p24-contacts-main">
				<div class="p24-container">
					<div class="p24-contacts-layout">

						<!-- Левая колонка: контактная информация -->
						<div class="p24-contacts-info">

							<div class="p24-contacts-block">
								<h3 class="p24-contacts-block__heading">Телефон</h3>
								<a href="tel:+74954144455" class="p24-contacts-block__value p24-contacts-block__value--lg">
									+7 (495) 414-44-55
								</a>
								<p class="p24-small">Пн — Пт, 9:00 — 18:00 (МСК)</p>
							</div>

							<div class="p24-contacts-block">
								<h3 class="p24-contacts-block__heading">Email</h3>
								<a href="mailto:info@pass24online.ru" class="p24-contacts-block__value">
									info@pass24online.ru
								</a>
							</div>

							<div class="p24-contacts-block">
								<h3 class="p24-contacts-block__heading">Мессенджеры</h3>
								<div class="p24-contacts-messengers">
									<a href="https://t.me/pass24" target="_blank" rel="noopener noreferrer" class="p24-contacts-messenger">
										<svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z"/></svg>
										Telegram
									</a>
								</div>
							</div>

							<div class="p24-contacts-block">
								<h3 class="p24-contacts-block__heading">Офис</h3>
								<p class="p24-contacts-block__value">Москва, Россия</p>
							</div>

							<div class="p24-contacts-block">
								<a href="/demo/" class="p24-btn p24-btn-primary" style="width: 100%; justify-content: center;">
									Запросить демонстрацию
								</a>
							</div>

						</div>


						<!-- Правая колонка: форма обратной связи -->
						<div class="p24-contacts-form-col">

							<div class="p24-contacts-form" id="p24-contacts-form">
								<h2 class="p24-h3" style="margin-bottom: 4px;">Напишите нам</h2>
								<p class="p24-small" style="margin-bottom: 24px;">Ответим в&nbsp;течение 2&nbsp;часов в&nbsp;рабочее время</p>

								<?php
								if ( function_exists( 'gravity_form' ) && class_exists( 'GFAPI' ) && GFAPI::get_form( 2 ) ) :
									gravity_form( 2, false, false, false, null, true, 0, true );
								else :
								?>

								<form id="p24-contact-form-el">
									<?php wp_nonce_field( 'pass24_contact', 'p24_contact_nonce' ); ?>

									<div class="p24-contacts-field">
										<label for="p24c-name">Имя <span class="p24-demo-required">*</span></label>
										<input type="text" id="p24c-name" name="name" placeholder="Алексей" required autocomplete="given-name">
									</div>

									<div class="p24-contacts-field-row">
										<div class="p24-contacts-field">
											<label for="p24c-email">Email <span class="p24-demo-required">*</span></label>
											<input type="email" id="p24c-email" name="email" placeholder="name@company.ru" required autocomplete="email">
										</div>
										<div class="p24-contacts-field">
											<label for="p24c-phone">Телефон</label>
											<input type="tel" id="p24c-phone" name="phone" placeholder="+7 (___) ___-__-__" autocomplete="tel">
										</div>
									</div>

									<div class="p24-contacts-field">
										<label for="p24c-subject">Тема</label>
										<select id="p24c-subject" name="subject">
											<option value="" disabled selected>Выберите тему</option>
											<option value="demo">Запрос демонстрации</option>
											<option value="pricing">Вопрос по тарифам</option>
											<option value="technical">Технический вопрос</option>
											<option value="partnership">Партнёрство</option>
											<option value="other">Другое</option>
										</select>
									</div>

									<div class="p24-contacts-field">
										<label for="p24c-message">Сообщение <span class="p24-demo-required">*</span></label>
										<textarea id="p24c-message" name="message" rows="4" placeholder="Опишите ваш вопрос или задачу" required></textarea>
									</div>

									<button type="submit" class="p24-btn p24-btn-primary p24-contacts-submit" style="width: 100%; justify-content: center;">
										Отправить сообщение
									</button>

									<p class="p24-small" style="text-align: center; margin-top: 12px; color: var(--p24-text-secondary);">
										Нажимая кнопку, вы&nbsp;соглашаетесь с&nbsp;<a href="/privacy/" style="color: var(--p24-primary);">политикой конфиденциальности</a>
									</p>
								</form>

								<!-- Success state -->
								<div class="p24-contacts-success" id="p24-contacts-success" style="display: none;">
									<div style="font-size: 48px; margin-bottom: 12px;">&#9989;</div>
									<h3 class="p24-h3" style="margin-bottom: 8px;">Сообщение отправлено</h3>
									<p class="p24-small">Мы ответим в&nbsp;течение 2&nbsp;часов в&nbsp;рабочее время</p>
								</div>

								<?php endif; ?>
							</div>

						</div>

					</div>
				</div>
			</section>


			<!-- ============================================================
			     КАРТА
			     ============================================================ -->

			<section class="p24-contacts-map">
				<div class="p24-contacts-map__embed">
					<!-- Яндекс.Карта или Google Maps embed — заменить на реальный код -->
					<div class="p24-contacts-map__placeholder">
						<div style="font-size: 48px; margin-bottom: 12px;">&#128205;</div>
						<p style="color: var(--p24-text-secondary);">Карта будет добавлена после указания точного адреса офиса</p>
					</div>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
