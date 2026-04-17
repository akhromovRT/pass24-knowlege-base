<?php
/**
 * Template: Mobile Pass Bureau / Мобильное бюро пропусков
 * @package PASS24_Child
 */
defined( 'ABSPATH' ) || exit;
require_once PASS24_CHILD_DIR . '/inc/product-data.php';
$product = pass24_get_product( 'mobile-pass-bureau' );
if ( ! $product ) { wp_redirect( home_url( '/products/' ) ); exit; }
require PASS24_CHILD_DIR . '/template-parts/product-page.php';
