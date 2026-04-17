<?php
/**
 * Template: Single Post (одна статья блога)
 *
 * Полноширинный макет: хлебные крошки, контент, related posts, CTA, навигация.
 *
 * @package PASS24_Child
 */

defined( 'ABSPATH' ) || exit;

add_filter( 'generate_show_sidebar', '__return_false', 99 );

get_header();

the_post();

// Время чтения
$word_count   = str_word_count( wp_strip_all_tags( get_the_content() ) );
$reading_time = max( 1, ceil( $word_count / 200 ) );

// Категории поста
$post_cats  = get_the_category();
$first_cat  = $post_cats ? $post_cats[0] : null;
?>

<div class="site-content" id="content">
	<div class="content-area" style="width: 100%;">
		<main class="site-main">


			<!-- BREADCRUMBS -->

			<section class="p24-blog-breadcrumbs-section">
				<div class="p24-container" style="max-width: 800px;">
					<nav class="p24-blog-breadcrumbs" aria-label="Хлебные крошки">
						<a href="<?php echo esc_url( get_permalink( get_option( 'page_for_posts' ) ) ?: home_url( '/blog/' ) ); ?>">Блог</a>
						<?php if ( $first_cat ) : ?>
						<span class="p24-blog-breadcrumbs__sep">/</span>
						<a href="<?php echo esc_url( get_category_link( $first_cat->term_id ) ); ?>"><?php echo esc_html( $first_cat->name ); ?></a>
						<?php endif; ?>
						<span class="p24-blog-breadcrumbs__sep">/</span>
						<span class="p24-blog-breadcrumbs__current"><?php the_title(); ?></span>
					</nav>
				</div>
			</section>


			<!-- ARTICLE HEADER -->

			<section class="p24-blog-article-header">
				<div class="p24-container" style="max-width: 800px;">
					<?php if ( $first_cat ) : ?>
					<a href="<?php echo esc_url( get_category_link( $first_cat->term_id ) ); ?>" class="p24-badge p24-badge-primary" style="margin-bottom: 16px; display: inline-block;"><?php echo esc_html( $first_cat->name ); ?></a>
					<?php endif; ?>
					<h1 class="p24-h1" style="margin-bottom: 16px;"><?php the_title(); ?></h1>
					<div class="p24-blog-article-meta">
						<time datetime="<?php echo get_the_date( 'c' ); ?>"><?php echo get_the_date(); ?></time>
						<span class="p24-blog-article-meta__sep">&middot;</span>
						<span><?php echo $reading_time; ?> мин чтения</span>
						<?php if ( $first_cat ) : ?>
						<span class="p24-blog-article-meta__sep">&middot;</span>
						<a href="<?php echo esc_url( get_category_link( $first_cat->term_id ) ); ?>" style="color: var(--p24-primary);"><?php echo esc_html( $first_cat->name ); ?></a>
						<?php endif; ?>
					</div>
				</div>
			</section>


			<!-- FEATURED IMAGE -->

			<?php if ( has_post_thumbnail() ) : ?>
			<section class="p24-section-sm">
				<div class="p24-container" style="max-width: 800px;">
					<div class="p24-blog-featured-img">
						<?php the_post_thumbnail( 'pass24-hero', [ 'loading' => 'eager' ] ); ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- ARTICLE CONTENT -->

			<section class="p24-section">
				<div class="p24-container" style="max-width: 800px;">
					<div class="p24-blog-content entry-content">
						<?php the_content(); ?>
					</div>
				</div>
			</section>


			<!-- INLINE CTA -->

			<section class="p24-section-sm p24-section-gray">
				<div class="p24-container" style="max-width: 800px;">
					<div class="p24-blog-inline-cta">
						<div>
							<strong style="font-size: 18px;">Попробуйте PASS24 &mdash; 14&nbsp;дней бесплатно</strong>
							<p style="color: var(--p24-text-secondary); margin: 4px 0 0;">Развёртывание за&nbsp;1&nbsp;час. Обучение включено.</p>
						</div>
						<a href="/demo/" class="p24-btn p24-btn-primary">Начать пилот</a>
					</div>
				</div>
			</section>


			<!-- RELATED POSTS -->

			<?php
			$related_args = [
				'posts_per_page' => 3,
				'post__not_in'   => [ get_the_ID() ],
				'orderby'        => 'rand',
			];
			if ( $first_cat ) {
				$related_args['cat'] = $first_cat->term_id;
			}
			$related = new WP_Query( $related_args );

			if ( $related->have_posts() ) :
			?>
			<section class="p24-section">
				<div class="p24-container">
					<div class="p24-section-header">
						<h2 class="p24-h2">Читайте также</h2>
					</div>
					<div class="p24-blog-grid p24-blog-grid--3" style="margin-top: 32px;">
						<?php while ( $related->have_posts() ) : $related->the_post(); ?>
						<a href="<?php the_permalink(); ?>" class="p24-card p24-blog-card">
							<?php if ( has_post_thumbnail() ) : ?>
							<div class="p24-blog-card__thumb">
								<?php the_post_thumbnail( 'pass24-card', [ 'loading' => 'lazy' ] ); ?>
							</div>
							<?php endif; ?>
							<div class="p24-blog-card__body">
								<?php
								$rel_cats = get_the_category();
								if ( $rel_cats ) :
								?>
								<span class="p24-blog-card__cat"><?php echo esc_html( $rel_cats[0]->name ); ?></span>
								<?php endif; ?>
								<h3 class="p24-h4 p24-blog-card__title"><?php the_title(); ?></h3>
								<div class="p24-blog-card__meta">
									<time datetime="<?php echo get_the_date( 'c' ); ?>"><?php echo get_the_date(); ?></time>
									<span class="p24-blog-card__read">Читать &rarr;</span>
								</div>
							</div>
						</a>
						<?php endwhile; wp_reset_postdata(); ?>
					</div>
				</div>
			</section>
			<?php endif; ?>


			<!-- POST NAVIGATION -->

			<section class="p24-section-sm p24-section-gray">
				<div class="p24-container" style="max-width: 800px;">
					<div class="p24-blog-nav">
						<?php
						$prev = get_previous_post();
						$next = get_next_post();
						?>
						<?php if ( $prev ) : ?>
						<a href="<?php echo get_permalink( $prev ); ?>" class="p24-blog-nav__link p24-blog-nav__link--prev">
							<span class="p24-blog-nav__label">&larr; Предыдущая</span>
							<span class="p24-blog-nav__title"><?php echo esc_html( $prev->post_title ); ?></span>
						</a>
						<?php else : ?>
						<div></div>
						<?php endif; ?>

						<?php if ( $next ) : ?>
						<a href="<?php echo get_permalink( $next ); ?>" class="p24-blog-nav__link p24-blog-nav__link--next">
							<span class="p24-blog-nav__label">Следующая &rarr;</span>
							<span class="p24-blog-nav__title"><?php echo esc_html( $next->post_title ); ?></span>
						</a>
						<?php else : ?>
						<div></div>
						<?php endif; ?>
					</div>
				</div>
			</section>


			<!-- CTA -->

			<section class="p24-section p24-section-dark">
				<div class="p24-container" style="text-align: center; max-width: 700px;">
					<h2 class="p24-h2" style="color: #fff;">Готовы автоматизировать доступ?</h2>
					<p class="p24-subtitle" style="color: rgba(255,255,255,.7); margin-bottom: 32px;">
						Запустите пилот PASS24 на&nbsp;вашем объекте&nbsp;&mdash; 14&nbsp;дней бесплатно, обучение включено.
					</p>
					<a href="/demo/" class="p24-btn p24-btn-accent p24-btn-lg">Начать пилот &mdash; 14 дней бесплатно</a>
				</div>
			</section>


		</main>
	</div>
</div>

<?php
get_footer();
