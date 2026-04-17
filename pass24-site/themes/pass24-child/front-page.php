<?php
/**
 * Template: Front Page (главная)
 *
 * WordPress автоматически использует front-page.php для статической главной страницы.
 * Полноширинный макет без сайдбара.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

// Отключить сайдбар
add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- ============================================================
			     СЕКЦИЯ 1 — HERO
			     ============================================================ -->

			<section class="p24-section" id="hero" style="padding-top: 100px;">
				<div class="p24-container p24-flex-row">

					<!-- Левая колонка: текст -->
					<div>
						<div style="margin-bottom: 24px;">
							<span class="p24-badge p24-badge-primary">&#127942; Лауреат премии &laquo;Новатор Москвы&raquo;</span>
							<span class="p24-badge" style="margin-left: 8px;">Реестр российского ПО</span>
						</div>

						<h1 class="p24-h1" style="margin-bottom: 24px;">
							Пропускной режим,<br>который работает сам
						</h1>

						<p class="p24-subtitle" style="margin-bottom: 32px;">
							300+ объектов по всей России — от элитных КП до логистических
							центров — уже перешли на&nbsp;PASS24.<br>
							1,5&nbsp;млн электронных пропусков каждый месяц.<br>
							Без серверов. Без бумаги. Без очередей.
						</p>

						<div style="display: flex; gap: 16px; flex-wrap: wrap;">
							<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Попробовать бесплатно — 14 дней</a>
							<a href="/demo/#booking" class="p24-btn p24-btn-secondary p24-btn-lg">Посмотреть демо за 15 минут</a>
						</div>

						<div class="p24-cta-checklist">
							<span class="p24-cta-checklist__item">Развёртывание за 1 час</span>
							<span class="p24-cta-checklist__item">Обучение включено</span>
							<span class="p24-cta-checklist__item">Без замены оборудования</span>
						</div>
					</div>

					<!-- Правая колонка: медиа -->
					<div>
						<img src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/hero-dashboard.webp' ); ?>"
						     alt="Интерфейс PASS24 — облачная система контроля доступа"
						     style="width: 100%; border-radius: var(--p24-radius-lg); box-shadow: var(--p24-shadow-lg);"
						     loading="eager"
						     onerror="this.style.display='none'">
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 2 — ПОЛОСА ДОВЕРИЯ
			     ============================================================ -->

			<section class="p24-section-sm p24-section-gray" id="trust-bar">
				<div class="p24-container">

					<!-- Бегущие логотипы -->
					<div class="p24-marquee">
						<div class="p24-marquee__track">
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
							// Дублируем для бесшовного скролла
							for ( $i = 0; $i < 2; $i++ ) :
								foreach ( $logos as $slug => $name ) :
							?>
								<img src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/logos/' . $slug . '.svg' ); ?>"
								     alt="<?php echo esc_attr( $name ); ?>"
								     loading="lazy"
								     onerror="this.style.display='none'">
							<?php
								endforeach;
							endfor;
							?>
						</div>
					</div>

					<!-- Цифры -->
					<div class="p24-stats" style="justify-content: center; margin-top: 24px;">
						<div>
							<div class="p24-stat-value">300+</div>
							<div class="p24-stat-label">объектов</div>
						</div>
						<div>
							<div class="p24-stat-value">1,5 млн</div>
							<div class="p24-stat-label">заявок / мес</div>
						</div>
						<div>
							<div class="p24-stat-value">18+ млн</div>
							<div class="p24-stat-label">заявок / год</div>
						</div>
						<div>
							<div class="p24-stat-value">&#128196;</div>
							<div class="p24-stat-label">Реестр российского ПО</div>
						</div>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 3 — ПРОБЛЕМЫ
			     ============================================================ -->

			<section class="p24-section" id="problems">
				<div class="p24-container">

					<div class="p24-section-header">
						<h2 class="p24-h2">Знакомые проблемы?</h2>
					</div>

					<div class="p24-grid-3">
						<div class="p24-card">
							<div style="font-size: 40px; margin-bottom: 16px;">&#128221;</div>
							<h3 class="p24-h4" style="margin-bottom: 12px;">Охрана записывает номера вручную</h3>
							<p class="p24-small">Ошибки, очереди, конфликты с&nbsp;жителями. Гости ждут на&nbsp;КПП по&nbsp;15-20&nbsp;минут.</p>
						</div>
						<div class="p24-card">
							<div style="font-size: 40px; margin-bottom: 16px;">&#128268;</div>
							<h3 class="p24-h4" style="margin-bottom: 12px;">Шлагбаум отдельно, домофон отдельно, камеры отдельно</h3>
							<p class="p24-small">Нет единой картины доступа по&nbsp;объекту. Данные в&nbsp;разных системах.</p>
						</div>
						<div class="p24-card">
							<div style="font-size: 40px; margin-bottom: 16px;">&#128187;</div>
							<h3 class="p24-h4" style="margin-bottom: 12px;">Серверы на объекте, кабели, ручные обновления</h3>
							<p class="p24-small">Высокая стоимость владения. Нет удалённого доступа. Масштабирование&nbsp;= новые серверы.</p>
						</div>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 4 — РЕШЕНИЕ (5 табов продуктов)
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="solution">
				<div class="p24-container">

					<div class="p24-section-header">
						<h2 class="p24-h2">Единая облачная платформа для всех точек доступа</h2>
					</div>

					<div class="p24-tabs" data-p24-tabs>
						<div class="p24-tabs__nav" role="tablist">
							<button class="p24-tabs__trigger is-active" data-tab="0" role="tab" aria-selected="true" aria-controls="product-0">Бюро пропусков</button>
							<button class="p24-tabs__trigger" data-tab="1" role="tab" aria-selected="false" aria-controls="product-1">Распознавание номеров</button>
							<button class="p24-tabs__trigger" data-tab="2" role="tab" aria-selected="false" aria-controls="product-2">Цифровой ключ</button>
							<button class="p24-tabs__trigger" data-tab="3" role="tab" aria-selected="false" aria-controls="product-3">Парковка</button>
							<button class="p24-tabs__trigger" data-tab="4" role="tab" aria-selected="false" aria-controls="product-4">Аналитика</button>
						</div>

						<div class="p24-tabs__panels">

							<div class="p24-tabs__panel is-active" id="product-0" role="tabpanel">
								<div class="p24-flex-row">
									<div>
										<h3 class="p24-h3" style="margin-bottom: 12px;">Мобильное бюро пропусков</h3>
										<p style="color: var(--p24-text-secondary); margin-bottom: 16px;">Заказ электронного пропуска за&nbsp;5&nbsp;секунд через мобильное приложение. Гость получает QR-код в&nbsp;WhatsApp или Telegram&nbsp;&mdash; без установки приложения.</p>
										<span class="p24-badge p24-badge-accent">5 секунд на оформление пропуска</span>
									</div>
									<div>
										<img src="/wp-content/uploads/products/mobile-pass.webp" alt="Мобильное бюро пропусков PASS24" style="width: 100%; border-radius: var(--p24-radius-lg);" loading="lazy" onerror="this.parentElement.style.display='none'">
									</div>
								</div>
							</div>

							<div class="p24-tabs__panel" id="product-1" role="tabpanel">
								<div class="p24-flex-row">
									<div>
										<h3 class="p24-h3" style="margin-bottom: 12px;">PASS24.авто</h3>
										<p style="color: var(--p24-text-secondary); margin-bottom: 16px;">Распознаёт госномера с&nbsp;точностью 98%&nbsp;&mdash; даже в&nbsp;полной темноте. Шлагбаум открывается автоматически для зарегистрированных авто.</p>
										<span class="p24-badge p24-badge-accent">98% точность распознавания</span>
									</div>
									<div>
										<img src="/wp-content/uploads/products/pass24-auto.webp" alt="PASS24.авто — распознавание номеров" style="width: 100%; border-radius: var(--p24-radius-lg);" loading="lazy" onerror="this.parentElement.style.display='none'">
									</div>
								</div>
							</div>

							<div class="p24-tabs__panel" id="product-2" role="tabpanel">
								<div class="p24-flex-row">
									<div>
										<h3 class="p24-h3" style="margin-bottom: 12px;">PASS24.Key</h3>
										<p style="color: var(--p24-text-secondary); margin-bottom: 16px;">Превращает смартфон в&nbsp;некопируемый ключ доступа. BLE и&nbsp;NFC&nbsp;HCE&nbsp;&mdash; привязан к&nbsp;конкретному устройству.</p>
										<span class="p24-badge p24-badge-accent">0 потерянных карт и брелоков</span>
									</div>
									<div>
										<img src="/wp-content/uploads/products/pass24-key.webp" alt="PASS24.Key — цифровой ключ" style="width: 100%; border-radius: var(--p24-radius-lg);" loading="lazy" onerror="this.parentElement.style.display='none'">
									</div>
								</div>
							</div>

							<div class="p24-tabs__panel" id="product-3" role="tabpanel">
								<div class="p24-flex-row">
									<div>
										<h3 class="p24-h3" style="margin-bottom: 12px;">PASS24.паркинг</h3>
										<p style="color: var(--p24-text-secondary); margin-bottom: 16px;">Автоматически распределяет квоты парковочных мест по&nbsp;адресам. Контроль заполненности, аналитика использования.</p>
										<span class="p24-badge p24-badge-accent">Автоматический контроль квот</span>
									</div>
									<div>
										<img src="/wp-content/uploads/products/pass24-parking.webp" alt="PASS24.паркинг — управление парковкой" style="width: 100%; border-radius: var(--p24-radius-lg);" loading="lazy" onerror="this.parentElement.style.display='none'">
									</div>
								</div>
							</div>

							<div class="p24-tabs__panel" id="product-4" role="tabpanel">
								<div class="p24-flex-row">
									<div>
										<h3 class="p24-h3" style="margin-bottom: 12px;">Аналитика и отчёты</h3>
										<p style="color: var(--p24-text-secondary); margin-bottom: 16px;">Все события в&nbsp;одном окне. Журнал проходов, статистика нагрузки, отчёты для&nbsp;УК и&nbsp;собственников.</p>
										<span class="p24-badge p24-badge-accent">Все данные в реальном времени</span>
									</div>
									<div>
										<img src="/wp-content/uploads/products/analytics.webp" alt="PASS24 — аналитика и отчёты" style="width: 100%; border-radius: var(--p24-radius-lg);" loading="lazy" onerror="this.parentElement.style.display='none'">
									</div>
								</div>
							</div>

						</div>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 5 — УДОБНО КАЖДОМУ (по ролям)
			     ============================================================ -->

			<section class="p24-section" id="roles">
				<div class="p24-container">

					<div class="p24-section-header">
						<h2 class="p24-h2">Удобно каждому участнику</h2>
					</div>

					<div class="p24-tabs" data-p24-tabs>
						<div class="p24-tabs__nav" role="tablist">
							<button class="p24-tabs__trigger is-active" data-tab="0" role="tab" aria-selected="true" aria-controls="role-0">Для жителей и арендаторов</button>
							<button class="p24-tabs__trigger" data-tab="1" role="tab" aria-selected="false" aria-controls="role-1">Для УК и владельцев</button>
							<button class="p24-tabs__trigger" data-tab="2" role="tab" aria-selected="false" aria-controls="role-2">Для охраны</button>
						</div>

						<div class="p24-tabs__panels">

							<div class="p24-tabs__panel is-active" id="role-0" role="tabpanel">
								<div class="p24-grid-2" style="max-width: 700px;">
									<div class="p24-card" style="padding: 20px;"><strong>Пропуск за 5 секунд</strong><p class="p24-small" style="margin-top: 4px;">Заказ без звонка охране</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Пропуск в мессенджер</strong><p class="p24-small" style="margin-top: 4px;">Отправка гостю в WhatsApp или Telegram</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Смартфон = ключ</strong><p class="p24-small" style="margin-top: 4px;">Вместо карт и брелоков</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Голосовой заказ</strong><p class="p24-small" style="margin-top: 4px;">Через Яндекс Алису</p></div>
								</div>
							</div>

							<div class="p24-tabs__panel" id="role-1" role="tabpanel">
								<div class="p24-grid-2" style="max-width: 700px;">
									<div class="p24-card" style="padding: 20px;"><strong>Единый дашборд</strong><p class="p24-small" style="margin-top: 4px;">Все объекты в одном окне</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Журнал и аналитика</strong><p class="p24-small" style="margin-top: 4px;">Полная история событий</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Меньше нагрузки на охрану</strong><p class="p24-small" style="margin-top: 4px;">Автоматизация рутины</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Привлекательность объекта</strong><p class="p24-small" style="margin-top: 4px;">Для жителей и арендаторов</p></div>
								</div>
							</div>

							<div class="p24-tabs__panel" id="role-2" role="tabpanel">
								<div class="p24-grid-2" style="max-width: 700px;">
									<div class="p24-card" style="padding: 20px;"><strong>Автоматическая проверка</strong><p class="p24-small" style="margin-top: 4px;">Без ручных журналов</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Мгновенные уведомления</strong><p class="p24-small" style="margin-top: 4px;">О нарушениях и инцидентах</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Правила по времени и зонам</strong><p class="p24-small" style="margin-top: 4px;">Чёткие правила доступа</p></div>
									<div class="p24-card" style="padding: 20px;"><strong>Мобильный пульт</strong><p class="p24-small" style="margin-top: 4px;">Управление со смартфона</p></div>
								</div>
							</div>

						</div>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 6 — РЕШЕНИЯ ПО СЕГМЕНТАМ (8 карточек)
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="segments">
				<div class="p24-container">

					<div class="p24-section-header">
						<h2 class="p24-h2">Решения для вашего типа объекта</h2>
					</div>

					<div class="p24-segments-grid">
						<?php
						$segments = [
							[ 'icon' => '&#127970;', 'title' => 'Жилые комплексы',         'desc' => 'Мобильные пропуска и распознавание номеров для жителей',         'clients' => 'Садовые Кварталы, Розмарин, Андерсен',          'url' => '/solutions/residential-complex/' ],
							[ 'icon' => '&#127969;', 'title' => 'Коттеджные посёлки',       'desc' => 'Автоматизация КПП с 98% распознаванием номеров',                 'clients' => 'Агаларов Эстейт, Росинка, Николино',            'url' => '/solutions/cottage-village/' ],
							[ 'icon' => '&#127963;', 'title' => 'Бизнес-центры',            'desc' => 'Цифровые ключи и контроль арендаторов',                          'clients' => 'Нагатино i-Land (МТС), ТехноПарк Цвет',        'url' => '/solutions/business-center/' ],
							[ 'icon' => '&#128666;', 'title' => 'Логистические комплексы',   'desc' => 'Контроль транспортного доступа 24/7',                            'clients' => 'MLP (1,7 млн м²), ФК Пульс (14 ЛЦ)',           'url' => '/solutions/logistics/' ],
							[ 'icon' => '&#127981;', 'title' => 'Производство',              'desc' => 'Учёт персонала, подрядчиков и транспорта',                       'clients' => 'МетМебель, Флексо-Пак, НПО Промпат',            'url' => '/solutions/production/' ],
							[ 'icon' => '&#127982;', 'title' => 'Индустриальные парки',      'desc' => 'Мультитенантный доступ с разграничением зон',                     'clients' => 'ЭЛМА Всеволжск, ИП Орёл, Предпортовый',        'url' => '/solutions/industrial-park/' ],
							[ 'icon' => '&#128679;', 'title' => 'Строительные площадки',     'desc' => 'Временные пропуска для подрядчиков',                             'clients' => '',                                               'url' => '/solutions/construction/' ],
							[ 'icon' => '&#128737;', 'title' => 'ЧОП',                       'desc' => 'Цифровые инструменты для службы охраны',                         'clients' => '',                                               'url' => '/solutions/security-company/' ],
						];
						foreach ( $segments as $seg ) :
						?>
						<a href="<?php echo esc_url( $seg['url'] ); ?>" class="p24-segment-card">
							<div class="p24-segment-card__icon"><?php echo $seg['icon']; ?></div>
							<div class="p24-segment-card__title"><?php echo esc_html( $seg['title'] ); ?></div>
							<div class="p24-segment-card__desc"><?php echo esc_html( $seg['desc'] ); ?></div>
							<?php if ( $seg['clients'] ) : ?>
								<div class="p24-segment-card__clients"><?php echo esc_html( $seg['clients'] ); ?></div>
							<?php endif; ?>
						</a>
						<?php endforeach; ?>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 7 — СРАВНЕНИЕ / ROI
			     ============================================================ -->

			<section class="p24-section" id="comparison">
				<div class="p24-container" style="max-width: 1000px;">

					<div class="p24-section-header">
						<h2 class="p24-h2">Облачная СКУД экономит до&nbsp;40% по&nbsp;сравнению с&nbsp;локальной</h2>
					</div>

					<div class="p24-table-responsive">
					<table class="p24-compare-table">
						<thead>
							<tr>
								<th>Параметр</th>
								<th>Традиционный СКУД</th>
								<th>PASS24.online</th>
							</tr>
						</thead>
						<tbody>
							<tr><td>Первоначальные затраты</td><td>от 500 000 &#8381;</td><td>от 0 &#8381; (SaaS)</td></tr>
							<tr><td>Ежемесячные расходы</td><td>Обслуживание серверов</td><td>от 10 990 &#8381;/мес</td></tr>
							<tr><td>Время развёртывания</td><td>2-4 недели</td><td>1 час</td></tr>
							<tr><td>Масштабирование</td><td>Новые серверы + кабели</td><td>Добавить объект в ЛК</td></tr>
							<tr><td>Удалённое управление</td><td>Нет</td><td>Да, из любой точки</td></tr>
							<tr><td>Мобильный доступ</td><td>Нет</td><td>iOS + Android</td></tr>
							<tr><td>Обновления ПО</td><td>Платные, ручные</td><td>Автоматические</td></tr>
							<tr><td>Привязка к вендору</td><td>Жёсткая</td><td>Open API</td></tr>
						</tbody>
					</table>
					</div><!-- /.p24-table-responsive -->

					<div style="text-align: center; margin-top: 32px;">
						<a href="/resources/roi-calculator/" class="p24-btn p24-btn-primary">Рассчитать экономию для вашего объекта</a>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 8 — КЕЙСЫ (карусель)
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="cases">
				<div class="p24-container">

					<div class="p24-section-header">
						<h2 class="p24-h2">Результаты наших клиентов</h2>
					</div>

					<div class="p24-carousel" data-p24-carousel>
						<div class="p24-carousel__track">

							<div class="p24-carousel__slide">
								<div class="p24-card" style="height: 100%;">
									<span class="p24-badge" style="margin-bottom: 16px;">Элитный КП</span>
									<h3 class="p24-h4" style="margin-bottom: 8px;">КП Агаларов Эстейт</h3>
									<p class="p24-small" style="margin-bottom: 16px;">330 га, гольф-поле</p>
									<p style="font-size: 20px; font-weight: 700; color: var(--p24-primary); margin-bottom: 16px;">Полная автоматизация КПП</p>
									<a href="/cases/agalarov/" class="p24-btn p24-btn-secondary p24-btn-sm">Подробнее</a>
								</div>
							</div>

							<div class="p24-carousel__slide">
								<div class="p24-card" style="height: 100%;">
									<span class="p24-badge" style="margin-bottom: 16px;">Логистика</span>
									<h3 class="p24-h4" style="margin-bottom: 8px;">MLP</h3>
									<p class="p24-small" style="margin-bottom: 16px;">1,7 млн м&sup2; складской недвижимости</p>
									<p style="font-size: 20px; font-weight: 700; color: var(--p24-primary); margin-bottom: 16px;">Контроль транспортного доступа</p>
									<a href="/cases/mlp/" class="p24-btn p24-btn-secondary p24-btn-sm">Подробнее</a>
								</div>
							</div>

							<div class="p24-carousel__slide">
								<div class="p24-card" style="height: 100%;">
									<span class="p24-badge" style="margin-bottom: 16px;">Элитный ЖК</span>
									<h3 class="p24-h4" style="margin-bottom: 8px;">ТСН Садовые Кварталы</h3>
									<p class="p24-small" style="margin-bottom: 16px;">De luxe, Хамовники</p>
									<p style="font-size: 20px; font-weight: 700; color: var(--p24-primary); margin-bottom: 16px;">Мобильные пропуска для жителей</p>
									<a href="/cases/sadovye-kvartaly/" class="p24-btn p24-btn-secondary p24-btn-sm">Подробнее</a>
								</div>
							</div>

							<div class="p24-carousel__slide">
								<div class="p24-card" style="height: 100%;">
									<span class="p24-badge" style="margin-bottom: 16px;">Развлечения</span>
									<h3 class="p24-h4" style="margin-bottom: 8px;">Live Арена Амфион</h3>
									<p class="p24-small" style="margin-bottom: 16px;">Арена на 11 000 мест</p>
									<p style="font-size: 20px; font-weight: 700; color: var(--p24-primary); margin-bottom: 16px;">Контроль доступа на мероприятиях</p>
									<a href="/cases/amphion/" class="p24-btn p24-btn-secondary p24-btn-sm">Подробнее</a>
								</div>
							</div>

						</div>
						<div class="p24-carousel__controls">
							<button class="p24-carousel__prev" aria-label="Предыдущий">&larr;</button>
							<div class="p24-carousel__dots"></div>
							<button class="p24-carousel__next" aria-label="Следующий">&rarr;</button>
						</div>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 9 — ОТЗЫВЫ (ротатор)
			     ============================================================ -->

			<section class="p24-section" id="testimonials">
				<div class="p24-container" style="max-width: 800px;">

					<div class="p24-section-header">
						<h2 class="p24-h2">Что говорят наши клиенты</h2>
					</div>

					<div class="p24-testimonials" data-p24-testimonials>

						<div class="p24-testimonials__item is-active">
							<blockquote class="p24-testimonials__quote">
								&laquo;За первый месяц количество звонков на&nbsp;пост охраны <strong>сократилось на&nbsp;70%</strong>. Жители сами заказывают пропуска, гости проходят по&nbsp;QR&nbsp;&mdash; <strong>охрана занимается безопасностью, а&nbsp;не&nbsp;бумагами.</strong>&raquo;
							</blockquote>
							<div class="p24-testimonials__author">
								<div>
									<div class="p24-testimonials__name">[Имя Фамилия]</div>
									<div class="p24-testimonials__role">[Должность], [Компания]</div>
								</div>
							</div>
						</div>

						<div class="p24-testimonials__item">
							<blockquote class="p24-testimonials__quote">
								&laquo;Развернули систему на&nbsp;нескольких объектах <strong>за&nbsp;один день</strong>. Водители проезжают на&nbsp;территорию без остановки&nbsp;&mdash; <strong>камера распознаёт номер и&nbsp;шлагбаум открывается автоматически.</strong>&raquo;
							</blockquote>
							<div class="p24-testimonials__author">
								<div>
									<div class="p24-testimonials__name">[Имя Фамилия]</div>
									<div class="p24-testimonials__role">[Должность], [Компания]</div>
								</div>
							</div>
						</div>

						<div class="p24-testimonials__item">
							<blockquote class="p24-testimonials__quote">
								&laquo;Жители оценили&nbsp;&mdash; <strong>больше не&nbsp;нужно носить карты и&nbsp;брелоки</strong>. Всё через телефон. А&nbsp;мы&nbsp;наконец получили <strong>полную аналитику по&nbsp;всем точкам доступа.</strong>&raquo;
							</blockquote>
							<div class="p24-testimonials__author">
								<div>
									<div class="p24-testimonials__name">[Имя Фамилия]</div>
									<div class="p24-testimonials__role">[Должность], [Компания]</div>
								</div>
							</div>
						</div>

					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 10 — ИНТЕГРАЦИИ
			     ============================================================ -->

			<section class="p24-section p24-section-gray" id="integrations">
				<div class="p24-container" style="max-width: 1000px;">

					<div class="p24-section-header">
						<h2 class="p24-h2">Работает с тем, что уже есть на&nbsp;объекте</h2>
						<p class="p24-subtitle" style="margin: 12px auto 0;">Подключаем к&nbsp;вашему текущему СКУД&nbsp;&mdash; без замены оборудования</p>
					</div>

					<div class="p24-logo-strip" style="justify-content: center; flex-wrap: wrap; gap: 32px;">
						<?php
						$integrations = [
							'trassir'      => 'Trassir',
							'ivideon'      => 'Ivideon',
							'sigur'        => 'Sigur',
							'dahua'        => 'Dahua',
							'cvs'          => 'CVS',
							'automarshal'  => 'Automarshal',
							'alice'        => 'Яндекс Алиса',
						];
						foreach ( $integrations as $slug => $name ) :
						?>
						<img src="<?php echo esc_url( PASS24_CHILD_URI . '/assets/img/integrations/' . $slug . '.svg' ); ?>"
						     alt="<?php echo esc_attr( $name ); ?>"
						     loading="lazy"
						     onerror="this.style.display='none'">
						<?php endforeach; ?>
					</div>

					<div style="text-align: center; margin-top: 32px;">
						<a href="/integrations/" class="p24-btn p24-btn-secondary">Открытый API для любых интеграций &rarr;</a>
					</div>

				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 11 — НАГРАДЫ И ДОВЕРИЕ
			     ============================================================ -->

			<section class="p24-section-sm" id="awards">
				<div class="p24-container">
					<div style="display: flex; flex-wrap: wrap; justify-content: center; gap: 16px;">
						<span class="p24-badge p24-badge-primary">&#127942; Новатор Москвы 2022</span>
						<span class="p24-badge p24-badge-primary">&#127942; ComNews Awards 2020</span>
						<span class="p24-badge">&#127942; Безопасность 2023</span>
						<span class="p24-badge">Единый реестр российского ПО</span>
						<span class="p24-badge">152-ФЗ</span>
						<span class="p24-badge">Серверы в РФ</span>
					</div>
				</div>
			</section>


			<!-- ============================================================
			     СЕКЦИЯ 12 — ФИНАЛЬНЫЙ CTA
			     ============================================================ -->

			<section class="p24-section p24-section-dark" id="final-cta">
				<div class="p24-container" style="max-width: 800px; text-align: center;">

					<h2 class="p24-h2" style="color: #FFFFFF; margin-bottom: 16px;">
						Запустите пилот&nbsp;&mdash; 14&nbsp;дней бесплатно
					</h2>

					<p style="font-size: 20px; color: rgba(255,255,255,0.7); margin-bottom: 32px;">
						Развёртывание за&nbsp;1&nbsp;час. Онлайн-обучение включено.<br>
						Без серверов. Без замены оборудования. Без&nbsp;риска.
					</p>

					<div style="display: flex; gap: 16px; justify-content: center; flex-wrap: wrap;">
						<a href="/demo/" class="p24-btn p24-btn-primary p24-btn-lg">Начать бесплатный пилот</a>
						<a href="/pricing/" class="p24-btn p24-btn-ghost p24-btn-lg">Рассчитать стоимость</a>
					</div>

					<div style="margin-top: 24px; display: flex; gap: 16px; justify-content: center; flex-wrap: wrap;">
						<a href="tel:+74954144455" style="color: rgba(255,255,255,0.7); text-decoration: none;">&#9742; +7 (495) 414-44-55</a>
						<a href="https://t.me/pass24" style="color: rgba(255,255,255,0.7); text-decoration: none;">Telegram</a>
					</div>

				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
