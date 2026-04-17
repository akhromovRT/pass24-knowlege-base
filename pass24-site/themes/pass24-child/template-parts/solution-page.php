<?php
/**
 * Template Part: Solution Page (8 секций)
 *
 * Переменная $solution должна быть определена перед include.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

if ( empty( $solution ) ) {
	return;
}

// Подключаем данные продуктов для секции 4.
require_once PASS24_CHILD_DIR . '/inc/product-data.php';
$all_products = pass24_get_products();

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- ============================================================
			     1. HERO
			     ============================================================ -->

			<section class="p24-section p24-solution-hero">
				<div class="p24-container">
					<div class="p24-solution-hero__layout">
						<div class="p24-solution-hero__content">
							<span class="p24-badge p24-badge-accent"><?php echo esc_html( $solution['name'] ); ?></span>
							<h1 class="p24-h1"><?php echo $solution['hero_title']; ?></h1>
							<p class="p24-subtitle"><?php echo $solution['hero_subtitle']; ?></p>
							<div class="p24-solution-hero__actions">
								<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Получить решение</a>
								<a href="#p24-solution-form" class="p24-btn p24-btn-secondary p24-btn-lg">Задать вопрос</a>
							</div>
						</div>
						<?php if ( ! empty( $solution['clients'] ) ) : ?>
						<div class="p24-solution-hero__clients">
							<p class="p24-small" style="margin-bottom: 12px; color: var(--p24-text-secondary);">Нам доверяют:</p>
							<ul class="p24-solution-clients-list">
								<?php foreach ( $solution['clients'] as $client ) : ?>
								<li><?php echo esc_html( $client ); ?></li>
								<?php endforeach; ?>
							</ul>
							<?php if ( ! empty( $solution['client_logos'] ) ) : ?>
							<div class="p24-solution-hero__logos">
								<?php foreach ( $solution['client_logos'] as $logo_slug ) : ?>
								<img
									src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/logos/' . $logo_slug . '.svg' ); ?>"
									alt="<?php echo esc_attr( $logo_slug ); ?>"
									height="32"
									loading="eager"
									onerror="this.style.display='none'"
								>
								<?php endforeach; ?>
							</div>
							<?php endif; ?>
						</div>
						<?php endif; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     2. ЗНАКОМЫЕ ПРОБЛЕМЫ?
			     ============================================================ -->

			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Знакомые проблемы?</h2>
					</div>
					<div class="p24-grid-2">
						<?php foreach ( $solution['pains'] as $pain ) : ?>
						<div class="p24-card p24-solution-pain">
							<div class="p24-solution-pain__icon">
								<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="var(--p24-primary)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
									<circle cx="12" cy="12" r="10"/>
									<line x1="12" y1="8" x2="12" y2="12"/>
									<line x1="12" y1="16" x2="12.01" y2="16"/>
								</svg>
							</div>
							<h3 class="p24-h4"><?php echo $pain['title']; ?></h3>
							<p><?php echo $pain['desc']; ?></p>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     3. КАК PASS24 РЕШАЕТ
			     ============================================================ -->

			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Как PASS24 решает эти задачи</h2>
					</div>
					<div class="p24-solution-map">
						<?php foreach ( $solution['solutions_map'] as $item ) : ?>
						<div class="p24-solution-map__row">
							<div class="p24-solution-map__pain">
								<span class="p24-solution-map__label">Проблема</span>
								<?php echo $item['pain']; ?>
							</div>
							<div class="p24-solution-map__arrow">&rarr;</div>
							<div class="p24-solution-map__fix">
								<span class="p24-solution-map__label">Решение</span>
								<?php echo $item['solution']; ?>
							</div>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     4. РЕЛЕВАНТНЫЕ ПРОДУКТЫ
			     ============================================================ -->

			<?php if ( ! empty( $solution['products'] ) ) : ?>
			<section class="p24-section p24-section-gray">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Продукты для вашего объекта</h2>
					</div>
					<div class="p24-grid-2">
						<?php foreach ( $solution['products'] as $prod_slug ) :
							$prod = $all_products[ $prod_slug ] ?? null;
							if ( ! $prod ) continue;
						?>
						<a href="/products/<?php echo esc_attr( $prod_slug ); ?>/" class="p24-card p24-solution-product-card">
							<span class="p24-badge p24-badge-primary"><?php echo $prod['badge']; ?></span>
							<h3 class="p24-h4" style="margin-top: 8px;"><?php echo esc_html( $prod['name'] ); ?></h3>
							<p class="p24-small"><?php echo wp_strip_all_tags( $prod['hero_subtitle'] ); ?></p>
							<span class="p24-solution-product-card__link">Подробнее &rarr;</span>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     5. КЕЙС
			     ============================================================ -->

			<?php if ( ! empty( $solution['case_study']['client'] ) ) : ?>
			<section class="p24-section">
				<div class="p24-container" style="max-width: 800px;">
					<div class="p24-solution-case">
						<span class="p24-badge p24-badge-primary"><?php echo esc_html( $solution['case_study']['segment'] ); ?></span>
						<h2 class="p24-h3" style="margin-top: 12px;"><?php echo esc_html( $solution['case_study']['client'] ); ?></h2>
						<blockquote class="p24-solution-case__quote">
							<?php echo $solution['case_study']['quote']; ?>
						</blockquote>
						<a href="<?php echo esc_url( $solution['case_study']['url'] ); ?>" class="p24-btn p24-btn-secondary">
							Читать кейс &rarr;
						</a>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     6. ROI-МЕТРИКИ
			     ============================================================ -->

			<?php if ( ! empty( $solution['roi_metrics'] ) ) : ?>
			<section class="p24-section p24-section-dark">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2" style="color: #fff;">Результаты внедрения</h2>
					</div>
					<div class="p24-solution-roi">
						<?php foreach ( $solution['roi_metrics'] as $metric ) : ?>
						<div class="p24-solution-roi__item">
							<div class="p24-solution-roi__value"><?php echo $metric['value']; ?></div>
							<div class="p24-solution-roi__label"><?php echo esc_html( $metric['label'] ); ?></div>
						</div>
						<?php endforeach; ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     7. ОТЗЫВ
			     ============================================================ -->

			<?php if ( ! empty( $solution['testimonial'] ) ) : ?>
			<section class="p24-section">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<blockquote class="p24-solution-testimonial">
						<p class="p24-solution-testimonial__text">&laquo;<?php echo $solution['testimonial']['quote']; ?>&raquo;</p>
						<footer class="p24-solution-testimonial__author">
							<strong><?php echo esc_html( $solution['testimonial']['name'] ); ?></strong>
							<span><?php echo esc_html( $solution['testimonial']['role'] ); ?></span>
						</footer>
					</blockquote>
				</div>
			</section>
			<?php endif; ?>


			<!-- ============================================================
			     8. CTA-ФОРМА
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="p24-solution-form">
				<div class="p24-container" style="max-width: 600px;">
					<div class="p24-section-header">
						<h2 class="p24-h2">Получить решение для вашего объекта</h2>
						<p class="p24-subtitle">Ответим в&nbsp;течение 2&nbsp;часов в&nbsp;рабочее время</p>
					</div>

					<?php
					if ( function_exists( 'gravity_form' ) && class_exists( 'GFAPI' ) && GFAPI::get_form( 3 ) ) :
						gravity_form( 3, false, false, false, [ 'segment' => $solution['form_segment'] ?? '' ], true, 0, true );
					else :
					?>

					<form id="p24-solution-form-el" class="p24-solution-form">
						<?php wp_nonce_field( 'pass24_solution', 'p24_solution_nonce' ); ?>
						<input type="hidden" name="segment" value="<?php echo esc_attr( $solution['form_segment'] ?? '' ); ?>">
						<input type="hidden" name="solution_name" value="<?php echo esc_attr( $solution['name'] ?? '' ); ?>">

						<div class="p24-contacts-field">
							<label for="p24s-name">Имя <span class="p24-demo-required">*</span></label>
							<input type="text" id="p24s-name" name="name" placeholder="Алексей" required autocomplete="given-name">
						</div>

						<div class="p24-contacts-field-row">
							<div class="p24-contacts-field">
								<label for="p24s-phone">Телефон <span class="p24-demo-required">*</span></label>
								<input type="tel" id="p24s-phone" name="phone" placeholder="+7 (___) ___-__-__" required autocomplete="tel">
							</div>
							<div class="p24-contacts-field">
								<label for="p24s-email">Email</label>
								<input type="email" id="p24s-email" name="email" placeholder="name@company.ru" autocomplete="email">
							</div>
						</div>

						<div class="p24-contacts-field">
							<label for="p24s-object">Тип объекта</label>
							<select id="p24s-object" name="object_type" data-p24-segment-field data-p24-auto-segment="<?php echo esc_attr( $solution['form_segment'] ?? '' ); ?>">
								<option value="" disabled>Выберите тип</option>
								<option value="zhk" <?php selected( $solution['form_segment'] ?? '', 'zhk' ); ?>>Жилой комплекс</option>
								<option value="kp" <?php selected( $solution['form_segment'] ?? '', 'kp' ); ?>>Коттеджный посёлок</option>
								<option value="bc" <?php selected( $solution['form_segment'] ?? '', 'bc' ); ?>>Бизнес-центр</option>
								<option value="logistics" <?php selected( $solution['form_segment'] ?? '', 'logistics' ); ?>>Логистический комплекс</option>
								<option value="production" <?php selected( $solution['form_segment'] ?? '', 'production' ); ?>>Производство</option>
								<option value="industrial" <?php selected( $solution['form_segment'] ?? '', 'industrial' ); ?>>Индустриальный парк</option>
								<option value="construction" <?php selected( $solution['form_segment'] ?? '', 'construction' ); ?>>Строительная площадка</option>
								<option value="security" <?php selected( $solution['form_segment'] ?? '', 'security' ); ?>>Охранное предприятие</option>
								<option value="other">Другое</option>
							</select>
						</div>

						<div class="p24-contacts-field">
							<label for="p24s-message">Комментарий</label>
							<textarea id="p24s-message" name="message" rows="3" placeholder="Опишите ваш объект или задачу"></textarea>
						</div>

						<button type="submit" class="p24-btn p24-btn-primary p24-contacts-submit" style="width: 100%; justify-content: center;">
							Отправить заявку
						</button>

						<p class="p24-small" style="text-align: center; margin-top: 12px; color: var(--p24-text-secondary);">
							Нажимая кнопку, вы&nbsp;соглашаетесь с&nbsp;<a href="/privacy/" style="color: var(--p24-primary);">политикой конфиденциальности</a>
						</p>
					</form>

					<!-- Success state -->
					<div class="p24-contacts-success" id="p24-solution-success" style="display: none;">
						<div style="font-size: 48px; margin-bottom: 12px;">&#9989;</div>
						<h3 class="p24-h3" style="margin-bottom: 8px;">Заявка отправлена</h3>
						<p class="p24-small">Мы ответим в&nbsp;течение 2&nbsp;часов в&nbsp;рабочее время</p>
					</div>

					<?php endif; ?>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
