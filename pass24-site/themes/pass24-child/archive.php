<?php
/**
 * Template: Blog Archive (список постов)
 *
 * Используется для /blog/, category, tag и date archives.
 * Полноширинный макет с фильтрацией по категориям.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();

$categories     = get_categories( [ 'hide_empty' => true ] );
$current_cat_id = is_category() ? get_queried_object_id() : 0;
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- HERO -->

			<section class="p24-blog-hero">
				<div class="p24-container" style="max-width: 700px; text-align: center;">
					<?php if ( is_category() ) : ?>
						<span class="p24-badge p24-badge-primary" style="margin-bottom: 12px;"><?php single_cat_title(); ?></span>
						<h1 class="p24-h1" style="margin-bottom: 16px;"><?php single_cat_title(); ?></h1>
						<?php if ( category_description() ) : ?>
						<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;"><?php echo category_description(); ?></p>
						<?php endif; ?>
					<?php elseif ( is_tag() ) : ?>
						<h1 class="p24-h1" style="margin-bottom: 16px;"><?php single_tag_title( '#' ); ?></h1>
					<?php else : ?>
						<h1 class="p24-h1" style="margin-bottom: 16px;">Блог PASS24</h1>
						<p class="p24-subtitle" style="max-width: 100%; margin: 0 auto;">
							Статьи о&nbsp;системах контроля доступа, автоматизации пропускного режима и&nbsp;безопасности объектов
						</p>
					<?php endif; ?>
				</div>
			</section>


			<!-- CATEGORY FILTER -->

			<?php if ( $categories ) : ?>
			<section class="p24-section-sm">
				<div class="p24-container">
					<div class="p24-blog-cats">
						<a href="<?php echo esc_url( get_permalink( get_option( 'page_for_posts' ) ) ?: home_url( '/blog/' ) ); ?>"
						   class="p24-blog-cats__item<?php echo $current_cat_id === 0 ? ' is-active' : ''; ?>">Все</a>
						<?php foreach ( $categories as $cat ) : ?>
						<a href="<?php echo esc_url( get_category_link( $cat->term_id ) ); ?>"
						   class="p24-blog-cats__item<?php echo $current_cat_id === $cat->term_id ? ' is-active' : ''; ?>">
							<?php echo esc_html( $cat->name ); ?>
						</a>
						<?php endforeach; ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- POSTS GRID -->

			<section class="p24-section">
				<div class="p24-container">
					<?php if ( have_posts() ) : ?>
					<div class="p24-blog-grid">
						<?php while ( have_posts() ) : the_post(); ?>
						<a href="<?php the_permalink(); ?>" class="p24-card p24-blog-card">
							<?php if ( has_post_thumbnail() ) : ?>
							<div class="p24-blog-card__thumb">
								<?php the_post_thumbnail( 'pass24-card', [ 'loading' => 'lazy' ] ); ?>
							</div>
							<?php endif; ?>
							<div class="p24-blog-card__body">
								<?php
								$post_cats = get_the_category();
								if ( $post_cats ) :
								?>
								<span class="p24-blog-card__cat"><?php echo esc_html( $post_cats[0]->name ); ?></span>
								<?php endif; ?>
								<h2 class="p24-h4 p24-blog-card__title"><?php the_title(); ?></h2>
								<?php if ( has_excerpt() || get_the_excerpt() ) : ?>
								<p class="p24-blog-card__excerpt"><?php echo wp_trim_words( get_the_excerpt(), 20 ); ?></p>
								<?php endif; ?>
								<div class="p24-blog-card__meta">
									<time datetime="<?php echo get_the_date( 'c' ); ?>"><?php echo get_the_date(); ?></time>
									<span class="p24-blog-card__read">Читать &rarr;</span>
								</div>
							</div>
						</a>
						<?php endwhile; ?>
					</div>

					<!-- PAGINATION -->
					<?php
					$pagination = paginate_links( [
						'type'      => 'array',
						'prev_text' => '&larr; Назад',
						'next_text' => 'Далее &rarr;',
					] );
					if ( $pagination ) :
					?>
					<nav class="p24-blog-pagination" aria-label="Навигация по записям">
						<?php foreach ( $pagination as $link ) : ?>
							<?php echo $link; ?>
						<?php endforeach; ?>
					</nav>
					<?php endif; ?>

					<?php else : ?>
					<div style="text-align: center; padding: 64px 0;">
						<p class="p24-subtitle">Записей пока нет. Скоро здесь появятся полезные статьи.</p>
					</div>
					<?php endif; ?>
				</div>
			</section>


			<!-- CTA -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Готовы автоматизировать доступ?</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						Запустите пилот PASS24 и&nbsp;оцените результаты за&nbsp;14&nbsp;дней.
					</p>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот &mdash; 14 дней бесплатно</a>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
