<?php
/**
 * Dispatcher: Садовые Кварталы — /cases/sadovye-kvartaly/
 */
defined( 'ABSPATH' ) || exit;
require_once PASS24_CHILD_DIR . '/inc/case-data.php';
$case = pass24_get_case( 'sadovye-kvartaly' );
if ( ! $case ) { wp_redirect( home_url( '/cases/' ) ); exit; }
require PASS24_CHILD_DIR . '/template-parts/case-page.php';
