<?php
/**
 * Template: Pricing Page (slug: pricing)
 *
 * Полноширинный шаблон страницы тарифов.
 * WordPress автоматически использует page-{slug}.php для страницы с соответствующим slug.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

// Отключить сайдбар на этой странице
add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">

			<!-- ============================================================
			     СЕКЦИЯ 1 — Заголовок + переключатель + тарифы
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header" style="text-align: center;">
						<h1 class="p24-h1">Тарифы PASS24</h1>
						<p class="p24-subtitle">
							Безлимитные пользователи и пропуска в каждом тарифе.<br>
							Выберите план под размер вашего объекта.
						</p>
					</div>

					<!-- Переключатель помесячно / за год -->
					<div class="p24-pricing-toggle" data-p24-pricing-toggle>
						<span class="p24-pricing-toggle__label is-active" data-period="monthly">Помесячно</span>
						<button class="p24-pricing-toggle__switch" aria-label="Переключить период оплаты"></button>
						<span class="p24-pricing-toggle__label" data-period="yearly">За год</span>
						<span class="p24-pricing-toggle__save">-10%</span>
					</div>

					<!-- Сетка тарифов -->
					<div class="p24-pricing-grid">

						<!-- БАЗОВЫЙ -->
						<div class="p24-plan">
							<div class="p24-plan__name">БАЗОВЫЙ</div>
							<div class="p24-plan__desc">Для небольших объектов</div>
							<div>
								<span class="p24-plan__price" data-price-monthly="10 990 ₽" data-price-yearly="9 891 ₽">10 990 ₽</span>
								<span class="p24-plan__price-period"> / мес</span>
							</div>
							<div class="p24-plan__price-yearly">9 891 ₽/мес при оплате за год</div>
							<div class="p24-plan__divider"></div>
							<ul class="p24-plan__features">
								<li>Мобильное приложение и веб-интерфейс</li>
								<li>Электронный журнал охраны</li>
								<li>Безлимитные пользователи</li>
								<li>Отчёты и аналитика</li>
								<li>Чёрные списки</li>
								<li>Рассылка уведомлений</li>
								<li>Кастомизация под регламенты</li>
								<li>Техподдержка</li>
								<li>Обучение системе</li>
							</ul>
							<div class="p24-plan__cta">
								<a href="/demo/" class="p24-btn p24-btn-secondary" style="width: 100%; text-align: center;">
									Попробовать бесплатно
								</a>
							</div>
						</div>

						<!-- СТАНДАРТНЫЙ -->
						<div class="p24-plan">
							<div class="p24-plan__name">СТАНДАРТНЫЙ</div>
							<div class="p24-plan__desc">Для жилых комплексов и посёлков</div>
							<div>
								<span class="p24-plan__price" data-price-monthly="14 990 ₽" data-price-yearly="13 491 ₽">14 990 ₽</span>
								<span class="p24-plan__price-period"> / мес</span>
							</div>
							<div class="p24-plan__price-yearly">13 491 ₽/мес при оплате за год</div>
							<div class="p24-plan__divider"></div>
							<ul class="p24-plan__features">
								<li>Всё из БАЗОВОГО, плюс:</li>
								<li>Несколько адресных групп</li>
								<li>Раздача гостевых пропусков из веба</li>
							</ul>
							<div class="p24-plan__cta">
								<a href="/demo/" class="p24-btn p24-btn-secondary" style="width: 100%; text-align: center;">
									Попробовать бесплатно
								</a>
							</div>
						</div>

						<!-- ПРОФЕССИОНАЛЬНЫЙ (рекомендуемый) -->
						<div class="p24-plan p24-plan--recommended">
							<div class="p24-plan__name">ПРОФЕССИОНАЛЬНЫЙ</div>
							<div class="p24-plan__desc">Интеграции и расширенные сценарии</div>
							<div>
								<span class="p24-plan__price" data-price-monthly="24 990 ₽" data-price-yearly="22 491 ₽">24 990 ₽</span>
								<span class="p24-plan__price-period"> / мес</span>
							</div>
							<div class="p24-plan__price-yearly">22 491 ₽/мес при оплате за год</div>
							<div class="p24-plan__divider"></div>
							<ul class="p24-plan__features">
								<li>Всё из СТАНДАРТНОГО, плюс:</li>
								<li>Распознавание номеров (LPR)</li>
								<li>Интеграция со СКУД</li>
								<li>Платный въезд/выезд</li>
								<li>Лимит мест на адрес</li>
								<li>Расширенные уведомления</li>
								<li>Контроль времени на территории</li>
								<li>Замена ТС</li>
							</ul>
							<div class="p24-plan__cta">
								<a href="/demo/" class="p24-btn p24-btn-primary" style="width: 100%; text-align: center;">
									Попробовать бесплатно
								</a>
							</div>
						</div>

						<!-- МУЛЬТИОБЪЕКТНЫЙ -->
						<div class="p24-plan">
							<div class="p24-plan__name">МУЛЬТИОБЪЕКТНЫЙ</div>
							<div class="p24-plan__desc">Для УК с несколькими объектами</div>
							<div>
								<span class="p24-plan__price" data-price-monthly="48 990 ₽" data-price-yearly="44 091 ₽">48 990 ₽</span>
								<span class="p24-plan__price-period"> / мес</span>
							</div>
							<div class="p24-plan__price-yearly">44 091 ₽/мес при оплате за год</div>
							<div class="p24-plan__divider"></div>
							<ul class="p24-plan__features">
								<li>Всё из ПРОФЕССИОНАЛЬНОГО, плюс:</li>
								<li>Мульти-объектность (зоны, арендаторы)</li>
								<li>Разграничение прав пользователей</li>
								<li>Персональный менеджер</li>
							</ul>
							<div class="p24-plan__cta">
								<a href="/demo/" class="p24-btn p24-btn-secondary" style="width: 100%; text-align: center;">
									Попробовать бесплатно
								</a>
							</div>
						</div>

					</div><!-- /.p24-pricing-grid -->
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 2 — Подключаемые модули
			     ============================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header" style="text-align: center;">
						<h2 class="p24-h2">Подключаемые модули</h2>
						<p class="p24-subtitle">Расширьте возможности любого тарифа</p>
					</div>

					<div class="p24-addons">
						<div class="p24-addon">
							<div class="p24-addon__info">
								<div class="p24-addon__name">PASS24.паркинг</div>
								<div class="p24-addon__desc">Управление парковочными местами: бронирование, оплата, контроль доступа по зонам.</div>
							</div>
							<a href="/demo/" class="p24-btn p24-btn-secondary">Узнать стоимость</a>
						</div>
						<div class="p24-addon">
							<div class="p24-addon__info">
								<div class="p24-addon__name">PASS24.Key</div>
								<div class="p24-addon__desc">Цифровой ключ доступа: открытие дверей, шлагбаумов и калиток со смартфона.</div>
							</div>
							<a href="/demo/" class="p24-btn p24-btn-secondary">Узнать стоимость</a>
						</div>
					</div>

					<div class="p24-addons" style="margin-top: 24px;">
						<div class="p24-addon">
							<div class="p24-addon__info">
								<div class="p24-addon__name">Приоритетная техподдержка</div>
								<div class="p24-addon__desc">Выделенная линия поддержки с ускоренным временем реакции.</div>
							</div>
							<a href="/demo/" class="p24-btn p24-btn-secondary">Узнать стоимость</a>
						</div>
						<div class="p24-addon">
							<div class="p24-addon__info">
								<div class="p24-addon__name">Кастомная разработка</div>
								<div class="p24-addon__desc">Разработка индивидуальных модулей и интеграций под ваши бизнес-процессы.</div>
							</div>
							<a href="/demo/" class="p24-btn p24-btn-secondary">Обсудить проект</a>
						</div>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 3 — Преимущества всех тарифов
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header" style="text-align: center;">
						<h2 class="p24-h2">Что входит в каждый тариф</h2>
					</div>

					<div class="p24-benefits-grid">
						<div class="p24-benefit">
							<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="var(--p24-primary)" stroke-width="1.5">
								<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/>
								<circle cx="9" cy="7" r="4"/>
								<path d="M22 21v-2a4 4 0 0 0-3-3.87"/>
								<path d="M16 3.13a4 4 0 0 1 0 7.75"/>
							</svg>
							<h3 class="p24-h4">Безлимитные пользователи</h3>
							<p class="p24-small" style="color: var(--p24-text-secondary);">Неограниченное количество жителей и пропусков без доплат</p>
						</div>

						<div class="p24-benefit">
							<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="var(--p24-primary)" stroke-width="1.5">
								<circle cx="12" cy="12" r="10"/>
								<polyline points="12 6 12 12 16 14"/>
							</svg>
							<h3 class="p24-h4">Запуск за 1 час</h3>
							<p class="p24-small" style="color: var(--p24-text-secondary);">Облачная система — не нужно менять оборудование</p>
						</div>

						<div class="p24-benefit">
							<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="var(--p24-primary)" stroke-width="1.5">
								<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
							</svg>
							<h3 class="p24-h4">Обучение и поддержка</h3>
							<p class="p24-small" style="color: var(--p24-text-secondary);">Онлайн-обучение команды и техподдержка в каждом тарифе</p>
						</div>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 4 — CTA: 14-дневный пилот
			     ============================================================ -->

			<section class="p24-section p24-section-dark" id="pilot">
				<div class="p24-container" style="text-align: center;">
					<h2 class="p24-h2" style="color: #FFFFFF;">Попробуйте PASS24 бесплатно — 14 дней</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,0.8); max-width: 600px; margin: 0 auto 32px;">
						Полный доступ к системе, обучение команды и персональная поддержка. Без замены оборудования.
					</p>
					<ul class="p24-cta-checklist" style="max-width: 400px; margin: 0 auto 32px;">
						<li>Полный доступ к системе</li>
						<li>Онлайн-обучение команды</li>
						<li>Персональная поддержка</li>
						<li>Запуск за 1 час</li>
						<li>Без замены оборудования</li>
					</ul>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Запустить пилот бесплатно</a>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 5 — FAQ
			     ============================================================ -->

			<section class="p24-section" id="pricing-faq">
				<div class="p24-container">
					<div class="p24-section-header" style="text-align: center;">
						<h2 class="p24-h2">Частые вопросы</h2>
					</div>

					<div class="p24-faq" data-p24-faq>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Что входит в каждый тариф?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
								</svg>
							</button>
							<div class="p24-faq__answer">
								<p>Все тарифы включают мобильное приложение, веб-интерфейс, электронный журнал охраны, безлимитных пользователей, отчёты и аналитику, чёрные списки, рассылку уведомлений, кастомизацию под регламенты, техподдержку и обучение системе. Различия — в дополнительных возможностях: от адресных групп (СТАНДАРТНЫЙ) до распознавания номеров и мульти-объектности (ПРОФЕССИОНАЛЬНЫЙ и МУЛЬТИОБЪЕКТНЫЙ).</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Как работает 14-дневный бесплатный пилот?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
								</svg>
							</button>
							<div class="p24-faq__answer">
								<p>Вы получаете полный доступ к системе PASS24 на 14 дней. Мы проводим онлайн-обучение вашей команды, подключаем персональную поддержку и запускаем систему на вашем объекте за 1 час. Замена оборудования не требуется — PASS24 работает с существующей инфраструктурой. После пилота вы выбираете подходящий тариф.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Можно ли масштабировать систему?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
								</svg>
							</button>
							<div class="p24-faq__answer">
								<p>Да. Вы можете перейти на более высокий тариф в любой момент. При переходе с БАЗОВОГО на СТАНДАРТНЫЙ или ПРОФЕССИОНАЛЬНЫЙ все данные сохраняются. Тариф МУЛЬТИОБЪЕКТНЫЙ поддерживает управление несколькими объектами из одной панели — идеально для управляющих компаний с портфелем жилых комплексов.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Как происходит миграция с другой системы?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
								</svg>
							</button>
							<div class="p24-faq__answer">
								<p>Миграция проходит без остановки работы объекта. Мы импортируем базу жителей и транспортных средств, настраиваем интеграции с существующим оборудованием и проводим обучение персонала. Весь процесс занимает от 1 дня в зависимости от размера объекта.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Что включено в обучение?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
								</svg>
							</button>
							<div class="p24-faq__answer">
								<p>В каждый тариф входит онлайн-обучение для сотрудников УК и охраны: работа с веб-панелью, мобильным приложением, электронным журналом, настройка правил и уведомлений. Обучение проводится в формате видеоконференции, запись сохраняется.</p>
							</div>
						</div>

						<div class="p24-faq__item">
							<button class="p24-faq__question">
								Какую скидку я получу при оплате за год?
								<svg class="p24-faq__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
								</svg>
							</button>
							<div class="p24-faq__answer">
								<p>При оплате за 12 месяцев вперёд вы получаете скидку 10% на весь период. Например, тариф ПРОФЕССИОНАЛЬНЫЙ вместо 24 990 ₽/мес будет стоить 22 491 ₽/мес — экономия 29 988 ₽ в год.</p>
							</div>
						</div>

					</div><!-- /.p24-faq -->
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 6 — Форма связи
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="contact-form">
				<div class="p24-container" style="max-width: 600px;">
					<div class="p24-section-header" style="text-align: center;">
						<h2 class="p24-h2">Остались вопросы?</h2>
						<p class="p24-subtitle">Оставьте заявку — менеджер свяжется в течение 15 минут в рабочее время</p>
					</div>

					<?php
					// Gravity Forms — подставить реальный ID формы после создания
					if ( function_exists( 'gravity_form' ) ) {
						gravity_form( 1, false, false, false, null, true );
					} else {
						?>
						<div style="background: var(--p24-bg-primary); border: 2px dashed var(--p24-border); border-radius: var(--p24-radius-lg); padding: 48px; text-align: center; color: var(--p24-text-secondary);">
							Форма заявки будет доступна после настройки Gravity Forms.<br>
							<small>Поля: Имя, Телефон, Email, Название объекта, Комментарий</small>
						</div>
						<?php
					}
					?>
				</div>
			</section>

		</main>
	</div>
</div>

<?php
get_footer();
