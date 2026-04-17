<?php
/**
 * Template: PASS24.Parking
 * @package PASS24_Child
 */
defined( 'ABSPATH' ) || exit;
require_once PASS24_CHILD_DIR . '/inc/product-data.php';
$product = pass24_get_product( 'pass24-parking' );
if ( ! $product ) { wp_redirect( home_url( '/products/' ) ); exit; }
require PASS24_CHILD_DIR . '/template-parts/product-page.php';
