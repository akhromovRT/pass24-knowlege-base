<?php
/**
 * Template: Business Center / Для бизнес-центров
 * @package PASS24_Child
 */
defined( 'ABSPATH' ) || exit;
require_once PASS24_CHILD_DIR . '/inc/solution-data.php';
$solution = pass24_get_solution( 'business-center' );
if ( ! $solution ) { wp_redirect( home_url( '/solutions/' ) ); exit; }
require PASS24_CHILD_DIR . '/template-parts/solution-page.php';
