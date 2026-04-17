<?php
/**
 * Template: Industrial Park / Для индустриальных парков
 * @package PASS24_Child
 */
defined( 'ABSPATH' ) || exit;
require_once PASS24_CHILD_DIR . '/inc/solution-data.php';
$solution = pass24_get_solution( 'industrial-park' );
if ( ! $solution ) { wp_redirect( home_url( '/solutions/' ) ); exit; }
require PASS24_CHILD_DIR . '/template-parts/solution-page.php';
