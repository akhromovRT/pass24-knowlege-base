<?php
/**
 * Template: About / О компании
 *
 * Страница о компании: миссия, цифры, награды, доверие, карта объектов.
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
			     СЕКЦИЯ 1 — HERO
			     ============================================================ -->

			<section class="p24-about-hero">
				<div class="p24-container" style="max-width: 800px; text-align: center;">
					<h1 class="p24-h1" style="margin-bottom: 16px;">
						Мы автоматизируем доступ для&nbsp;300+ объектов по&nbsp;всей России
					</h1>
					<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
						PASS24.online&nbsp;&mdash; облачная платформа управления доступом.
						Собственная разработка, включённая в&nbsp;Единый реестр российского&nbsp;ПО.
					</p>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 2 — ЦИФРЫ
			     ============================================================ -->

			<section class="p24-section-sm p24-section-gray">
				<div class="p24-container">
					<div class="p24-about-stats">
						<div class="p24-about-stat">
							<div class="p24-about-stat__value">300+</div>
							<div class="p24-about-stat__label">объектов по&nbsp;всей России</div>
						</div>
						<div class="p24-about-stat">
							<div class="p24-about-stat__value">1,5&nbsp;млн</div>
							<div class="p24-about-stat__label">электронных заявок в&nbsp;месяц</div>
						</div>
						<div class="p24-about-stat">
							<div class="p24-about-stat__value">6</div>
							<div class="p24-about-stat__label">продуктов в&nbsp;экосистеме</div>
						</div>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 3 — О ПЛАТФОРМЕ
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container" style="max-width: 900px;">
					<div class="p24-section-header">
						<h2 class="p24-h2">Облачная СКУД нового поколения</h2>
					</div>

					<div class="p24-about-features">
						<div class="p24-about-feature">
							<div class="p24-about-feature__icon">&#9729;&#65039;</div>
							<h3 class="p24-h4">Облачная архитектура</h3>
							<p class="p24-small">Без серверов на объекте. Развёртывание за&nbsp;1&nbsp;час. Управление из&nbsp;любой точки мира через браузер или мобильное приложение.</p>
						</div>
						<div class="p24-about-feature">
							<div class="p24-about-feature__icon">&#128256;</div>
							<h3 class="p24-h4">Экосистема продуктов</h3>
							<p class="p24-small">Мобильное бюро пропусков, распознавание номеров, цифровые ключи, парковка, аналитика&nbsp;&mdash; всё в&nbsp;единой платформе.</p>
						</div>
						<div class="p24-about-feature">
							<div class="p24-about-feature__icon">&#128279;</div>
							<h3 class="p24-h4">Открытые интеграции</h3>
							<p class="p24-small">Trassir, Ivideon, Sigur, Dahua, Automarshal, Яндекс&nbsp;Алиса. Open&nbsp;API для подключения любых систем.</p>
						</div>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 4 — НАГРАДЫ
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="awards">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Награды и признание</h2>
					</div>

					<div class="p24-about-awards">

						<div class="p24-about-award">
							<div class="p24-about-award__badge">&#127942;</div>
							<div class="p24-about-award__year">2022</div>
							<h3 class="p24-h4 p24-about-award__title">Премия Мэра Москвы &laquo;Новатор Москвы&raquo;</h3>
							<p class="p24-small">Финалист в&nbsp;номинации &laquo;Лидеры инноваций&raquo;, направление &laquo;Транспорт и&nbsp;логистика&raquo;. Подписано С.С.&nbsp;Собяниным.</p>
							<span class="p24-badge p24-badge-primary" style="margin-top: 12px;">Правительство Москвы</span>
						</div>

						<div class="p24-about-award">
							<div class="p24-about-award__badge">&#127942;</div>
							<div class="p24-about-award__year">2020</div>
							<h3 class="p24-h4 p24-about-award__title">ComNews Awards</h3>
							<p class="p24-small">Лауреат&nbsp;&mdash; &laquo;Лучшее решение по&nbsp;цифровым пропускам&raquo;. Профессиональная премия в&nbsp;области телекоммуникаций и&nbsp;цифровых технологий.</p>
							<span class="p24-badge" style="margin-top: 12px;">ComNews</span>
						</div>

						<div class="p24-about-award">
							<div class="p24-about-award__badge">&#127942;</div>
							<div class="p24-about-award__year">2023</div>
							<h3 class="p24-h4 p24-about-award__title">Безопасность</h3>
							<p class="p24-small">Партнёр профессиональной конференции по&nbsp;безопасности. Презентация облачных решений для&nbsp;контроля доступа.</p>
							<span class="p24-badge" style="margin-top: 12px;">Конференция</span>
						</div>

					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 5 — ДОВЕРИЕ И СООТВЕТСТВИЕ
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container" style="max-width: 900px;">
					<div class="p24-section-header">
						<h2 class="p24-h2">Безопасность и соответствие</h2>
					</div>

					<div class="p24-about-trust">

						<div class="p24-about-trust-card">
							<div class="p24-about-trust-card__icon">
								<img src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/icons/registry-badge.svg' ); ?>"
								     alt="Реестр российского ПО" width="48" height="48"
								     onerror="this.parentElement.textContent='&#128196;'">
							</div>
							<h3 class="p24-h4">Единый реестр российского&nbsp;ПО</h3>
							<p class="p24-small">PASS24 включён в&nbsp;Единый реестр отечественного программного обеспечения. Соответствует требованиям импортозамещения.</p>
						</div>

						<div class="p24-about-trust-card">
							<div class="p24-about-trust-card__icon">&#128274;</div>
							<h3 class="p24-h4">152-ФЗ о&nbsp;персональных данных</h3>
							<p class="p24-small">Полное соответствие Федеральному закону &numero;&nbsp;152-ФЗ. Обработка и&nbsp;хранение данных в&nbsp;соответствии с&nbsp;законодательством&nbsp;РФ.</p>
						</div>

						<div class="p24-about-trust-card">
							<div class="p24-about-trust-card__icon">&#127479;&#127482;</div>
							<h3 class="p24-h4">Серверы на&nbsp;территории&nbsp;РФ</h3>
							<p class="p24-small">Вся инфраструктура расположена в&nbsp;российских дата-центрах. Данные не&nbsp;покидают территорию Российской Федерации.</p>
						</div>

					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 6 — НАМ ДОВЕРЯЮТ
			     ============================================================ -->

			<section class="p24-section-sm p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header" style="margin-bottom: 32px;">
						<h2 class="p24-h2">Нам доверяют</h2>
					</div>

					<div class="p24-logo-strip" style="justify-content: center; flex-wrap: wrap; gap: 40px;">
						<?php
						$logos = [
							'mts'              => 'МТС',
							'mlp'              => 'MLP',
							'sezar'            => 'Sezar Group',
							'sadovye-kvartaly' => 'Садовые Кварталы',
							'agalarov'         => 'Агаларов Эстейт',
							'rosinka'          => 'Росинка',
							'amphion'          => 'Live Арена Амфион',
						];
						foreach ( $logos as $slug => $name ) :
						?>
							<img src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/logos/' . $slug . '.svg' ); ?>"
							     alt="<?php echo esc_attr( $name ); ?>"
							     loading="lazy"
							     onerror="this.style.display='none'">
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 7 — КАРТА ОБЪЕКТОВ (placeholder)
			     ============================================================ -->

			<section class="p24-section" id="map">
				<div class="p24-container" style="max-width: 900px;">
					<div class="p24-section-header">
						<h2 class="p24-h2">География присутствия</h2>
						<p class="p24-subtitle" style="margin: 12px auto 0;">
							300+ объектов от&nbsp;Калининграда до&nbsp;Владивостока
						</p>
					</div>

					<div class="p24-about-map-placeholder">
						<div class="p24-about-map-placeholder__inner">
							<div style="font-size: 48px; margin-bottom: 16px;">&#127758;</div>
							<p style="font-size: 16px; color: var(--p24-text-secondary);">
								Интерактивная карта объектов будет добавлена<br>после получения согласия клиентов
							</p>
						</div>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 8 — ФИНАЛЬНЫЙ CTA
			     ============================================================ -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="max-width: 800px; text-align: center;">
					<h2 class="p24-h2" style="color: #FFFFFF; margin-bottom: 16px;">
						Начните пилотный проект
					</h2>
					<p style="font-size: 20px; color: rgba(255,255,255,0.7); margin-bottom: 32px;">
						14 дней бесплатно. Развёртывание за&nbsp;1&nbsp;час.<br>
						Обучение и&nbsp;онлайн-сопровождение включены.
					</p>
					<div style="display: flex; gap: 16px; justify-content: center; flex-wrap: wrap;">
						<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Попробовать бесплатно — 14 дней</a>
						<a href="/pricing/" class="p24-btn p24-btn-ghost p24-btn-lg">Посмотреть тарифы</a>
					</div>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
