<?php
/**
 * Template: Demo / Pilot Landing Page
 *
 * Конверсионная страница: 3-шаговая форма запроса демо.
 * Если Gravity Forms установлен — использует его, иначе — standalone HTML-форма.
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

			<section class="p24-demo-hero">
				<div class="p24-container">
					<div class="p24-demo-hero__badges">
						<span class="p24-badge p24-badge-primary">&#127942; Новатор Москвы 2022</span>
						<span class="p24-badge">Реестр российского ПО</span>
					</div>
					<h1 class="p24-h1 p24-demo-hero__title">
						Посмотрите, как PASS24 работает<br>на&nbsp;вашем типе объекта
					</h1>
					<p class="p24-subtitle p24-demo-hero__subtitle">
						15-минутная персональная демонстрация.<br>
						Покажем решение именно для ваших задач.
					</p>
				</div>
			</section>


			<!-- ============================================================
			     ОСНОВНОЙ БЛОК: ФОРМА + BENEFITS
			     ============================================================ -->

			<section class="p24-section p24-demo-main" id="demo-form-section">
				<div class="p24-container">
					<div class="p24-demo-layout">

						<!-- Левая колонка: форма -->
						<div class="p24-demo-form-col">

							<?php
							// Если Gravity Forms установлен и форма ID=1 существует — использовать GF
							if ( function_exists( 'gravity_form' ) && class_exists( 'GFAPI' ) && GFAPI::get_form( 1 ) ) :
								gravity_form( 1, false, false, false, null, true, 0, true );
							else :
							?>

							<!-- Standalone multi-step форма -->
							<div class="p24-demo-form" id="p24-demo-form">

								<!-- Progress bar -->
								<div class="p24-demo-progress">
									<div class="p24-demo-progress__bar">
										<div class="p24-demo-progress__fill" id="p24-progress-fill" style="width: 33.3%"></div>
									</div>
									<div class="p24-demo-progress__steps">
										<span class="p24-demo-progress__step is-active" data-step="1">1. Контакты</span>
										<span class="p24-demo-progress__step" data-step="2">2. Ваш объект</span>
										<span class="p24-demo-progress__step" data-step="3">3. Время звонка</span>
									</div>
								</div>

								<form id="p24-demo-form-el" novalidate>

									<?php wp_nonce_field( 'pass24_demo_request', 'p24_demo_nonce' ); ?>

									<!-- Шаг 1: Контакты -->
									<div class="p24-demo-step is-active" data-step="1">
										<h3 class="p24-h3 p24-demo-step__title">Как с вами связаться?</h3>

										<div class="p24-demo-field">
											<label for="p24-name">Имя <span class="p24-demo-required">*</span></label>
											<input type="text" id="p24-name" name="name" placeholder="Алексей" required autocomplete="given-name">
											<span class="p24-demo-field__error"></span>
										</div>

										<div class="p24-demo-field">
											<label for="p24-phone">Телефон <span class="p24-demo-required">*</span></label>
											<input type="tel" id="p24-phone" name="phone" placeholder="+7 (___) ___-__-__" required autocomplete="tel">
											<span class="p24-demo-field__error"></span>
										</div>

										<div class="p24-demo-field">
											<label for="p24-email">Email <span class="p24-demo-required">*</span></label>
											<input type="email" id="p24-email" name="email" placeholder="name@company.ru" required autocomplete="email">
											<span class="p24-demo-field__error"></span>
										</div>

										<button type="button" class="p24-btn p24-btn-primary p24-demo-next" data-next="2" style="width: 100%; justify-content: center;">
											Далее &rarr;
										</button>
									</div>


									<!-- Шаг 2: Квалификация -->
									<div class="p24-demo-step" data-step="2">
										<h3 class="p24-h3 p24-demo-step__title">Расскажите о вашем объекте</h3>

										<div class="p24-demo-field">
											<label for="p24-object-type">Тип объекта <span class="p24-demo-required">*</span></label>
											<select id="p24-object-type" name="object_type" required>
												<option value="" disabled selected>Выберите тип</option>
												<option value="zhk">Жилой комплекс</option>
												<option value="kp">Коттеджный посёлок</option>
												<option value="bc">Бизнес-центр</option>
												<option value="logistics">Склад / Логистический комплекс</option>
												<option value="production">Производство</option>
												<option value="industrial">Индустриальный парк</option>
												<option value="construction">Строительная площадка</option>
												<option value="security">ЧОП</option>
												<option value="other">Другое</option>
											</select>
											<span class="p24-demo-field__error"></span>
										</div>

										<div class="p24-demo-field">
											<label>Количество точек доступа <span class="p24-demo-required">*</span></label>
											<div class="p24-demo-radio-group" id="p24-access-points">
												<label class="p24-demo-radio">
													<input type="radio" name="access_points" value="1-5" required>
													<span>1 &ndash; 5</span>
												</label>
												<label class="p24-demo-radio">
													<input type="radio" name="access_points" value="5-20">
													<span>5 &ndash; 20</span>
												</label>
												<label class="p24-demo-radio">
													<input type="radio" name="access_points" value="20-50">
													<span>20 &ndash; 50</span>
												</label>
												<label class="p24-demo-radio">
													<input type="radio" name="access_points" value="50+">
													<span>50+</span>
												</label>
											</div>
											<span class="p24-demo-field__error"></span>
										</div>

										<div class="p24-demo-field">
											<label>Есть ли текущий СКУД?</label>
											<div class="p24-demo-radio-group">
												<label class="p24-demo-radio">
													<input type="radio" name="has_skud" value="yes">
													<span>Да, хотим заменить / дополнить</span>
												</label>
												<label class="p24-demo-radio">
													<input type="radio" name="has_skud" value="no">
													<span>Нет, ставим впервые</span>
												</label>
											</div>
										</div>

										<div class="p24-demo-buttons">
											<button type="button" class="p24-btn p24-btn-secondary p24-demo-prev" data-prev="1">
												&larr; Назад
											</button>
											<button type="button" class="p24-btn p24-btn-primary p24-demo-next" data-next="3">
												Далее &rarr;
											</button>
										</div>
									</div>


									<!-- Шаг 3: Время звонка -->
									<div class="p24-demo-step" data-step="3">
										<h3 class="p24-h3 p24-demo-step__title">Когда вам удобно?</h3>

										<div class="p24-demo-field">
											<label for="p24-call-time">Предпочтительное время</label>
											<select id="p24-call-time" name="call_time">
												<option value="" disabled selected>Выберите время</option>
												<option value="morning">Утро (9:00 — 12:00)</option>
												<option value="afternoon">День (12:00 — 15:00)</option>
												<option value="evening">Вечер (15:00 — 18:00)</option>
												<option value="any">Любое время</option>
											</select>
										</div>

										<div class="p24-demo-field">
											<label for="p24-comment">Комментарий <span class="p24-demo-optional">(необязательно)</span></label>
											<textarea id="p24-comment" name="comment" rows="3" placeholder="Расскажите подробнее о задаче, если хотите"></textarea>
										</div>

										<div class="p24-demo-buttons">
											<button type="button" class="p24-btn p24-btn-secondary p24-demo-prev" data-prev="2">
												&larr; Назад
											</button>
											<button type="submit" class="p24-btn p24-btn-primary p24-demo-submit" style="flex: 1;">
												Запросить демо
											</button>
										</div>
									</div>

								</form>
							</div>

							<?php endif; ?>

						</div>


						<!-- Правая колонка: benefits -->
						<div class="p24-demo-benefits-col">

							<div class="p24-demo-benefits">
								<h3 class="p24-h4" style="margin-bottom: 20px;">Что вы получите</h3>
								<ul class="p24-demo-benefits__list">
									<li>Персональную демонстрацию под ваш тип объекта</li>
									<li>Ответы на технические вопросы</li>
									<li>Расчёт стоимости для вашего случая</li>
									<li>Доступ к пилоту на 14 дней &mdash; бесплатно</li>
								</ul>
							</div>

							<div class="p24-demo-trust">
								<div class="p24-demo-trust__badges">
									<span class="p24-badge">300+ объектов</span>
									<span class="p24-badge">1,5 млн заявок/мес</span>
									<span class="p24-badge">Реестр РФ ПО</span>
								</div>
							</div>

							<div class="p24-demo-quote">
								<blockquote>
									&laquo;Развернули за&nbsp;48&nbsp;часов на&nbsp;нескольких объектах. Водители проезжают без остановки&nbsp;&mdash; камера распознаёт номер автоматически.&raquo;
								</blockquote>
								<div class="p24-demo-quote__author">
									<div class="p24-demo-quote__name">[Имя Фамилия]</div>
									<div class="p24-demo-quote__role">[Должность], [Компания]</div>
								</div>
							</div>

							<div class="p24-demo-contact">
								<p>Предпочитаете позвонить?</p>
								<a href="tel:+74954144455" class="p24-demo-contact__phone">+7 (495) 414-44-55</a>
								<div class="p24-demo-contact__messengers">
									<a href="https://t.me/pass24" target="_blank" rel="noopener noreferrer">Telegram</a>
								</div>
							</div>

						</div>

					</div>
				</div>
			</section>


			<!-- ============================================================
			     THANK YOU STATE (скрыт, показывается после отправки)
			     ============================================================ -->

			<section class="p24-section p24-demo-thankyou" id="p24-demo-thankyou" style="display: none;">
				<div class="p24-container" style="max-width: 680px; text-align: center;">
					<div style="font-size: 64px; margin-bottom: 16px;">&#9989;</div>
					<h2 class="p24-h2" style="margin-bottom: 16px;">Заявка отправлена!</h2>
					<p class="p24-subtitle" style="max-width: 100%; margin-bottom: 32px;">
						Наш менеджер свяжется с вами в&nbsp;течение 2&nbsp;часов в&nbsp;рабочее время.<br>
						Подготовим демонстрацию под ваш тип объекта.
					</p>

					<div class="p24-demo-thankyou__steps">
						<div class="p24-demo-thankyou__step">
							<div class="p24-demo-thankyou__num">1</div>
							<div>
								<strong>Звонок менеджера</strong>
								<p class="p24-small">Уточним детали и&nbsp;подберём время</p>
							</div>
						</div>
						<div class="p24-demo-thankyou__step">
							<div class="p24-demo-thankyou__num">2</div>
							<div>
								<strong>Персональная демонстрация</strong>
								<p class="p24-small">15 минут, онлайн, под ваш тип объекта</p>
							</div>
						</div>
						<div class="p24-demo-thankyou__step">
							<div class="p24-demo-thankyou__num">3</div>
							<div>
								<strong>Бесплатный пилот 14 дней</strong>
								<p class="p24-small">Развёртывание за 1 час + обучение</p>
							</div>
						</div>
					</div>

					<div style="margin-top: 32px;">
						<a href="/" class="p24-btn p24-btn-secondary">Вернуться на главную</a>
					</div>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
