<?php
/**
 * Plugin Name: PASS24 AI Sales Factory Integration
 * Description: Регистрирует мета-поля Rank Math в REST API и дублирует лиды в AI Sales Factory.
 * Version:     1.0.0
 *
 * Установка: скопировать в wp-content/mu-plugins/ — активируется автоматически.
 *
 * Требует в wp-config.php:
 *   define( 'AI_FACTORY_URL', 'http://5.42.101.27:8000' );
 */

if ( ! defined( 'ABSPATH' ) ) {
    exit;
}

// Enable Application Passwords (Wordfence disables them by default).
// Required for AI Sales Factory to publish content via REST API.
add_filter( 'wp_is_application_passwords_available', '__return_true', 999 );

// ---------------------------------------------------------------------------
// 1. Регистрация мета-полей Rank Math Pro в WordPress REST API
//
//    По умолчанию Rank Math не регистрирует свои мета-поля в REST API,
//    из-за чего pass24-ai-sales/src/core/tools/wordpress.py не может
//    записать rank_math_title / rank_math_description при публикации.
//    Этот блок открывает поля для write-доступа через /wp/v2/posts.
// ---------------------------------------------------------------------------
add_action( 'init', function () {
    $rank_math_fields = [
        'rank_math_title'         => 'SEO-заголовок страницы (Rank Math)',
        'rank_math_description'   => 'SEO мета-описание страницы (Rank Math)',
        'rank_math_focus_keyword' => 'Главное ключевое слово (Rank Math)',
    ];

    foreach ( $rank_math_fields as $key => $description ) {
        register_post_meta( 'post', $key, [
            'show_in_rest'  => true,
            'single'        => true,
            'type'          => 'string',
            'description'   => $description,
            'auth_callback' => fn() => current_user_can( 'edit_posts' ),
        ] );
    }
} );

// ---------------------------------------------------------------------------
// 2. Дублирование лидов в AI Sales Factory
//
//    Хукается на pass24_lead_submitted (вызывается из functions.php при
//    отправке демо-формы или контактной формы). Fire-and-forget.
//    Основной поток → Битрикс24 не затрагивается.
// ---------------------------------------------------------------------------
add_action( 'pass24_lead_submitted', function ( array $lead_data ) {
    $ai_factory_url = defined( 'AI_FACTORY_URL' ) ? AI_FACTORY_URL : getenv( 'AI_FACTORY_URL' );

    if ( empty( $ai_factory_url ) ) {
        return; // AI_FACTORY_URL не задан — пропустить без ошибки
    }

    wp_remote_post(
        trailingslashit( $ai_factory_url ) . 'webhooks/lead-form',
        [
            'body'      => wp_json_encode( $lead_data ),
            'headers'   => [ 'Content-Type' => 'application/json' ],
            'timeout'   => 5,
            'blocking'  => false,   // fire-and-forget: не задерживает ответ пользователю
            'sslverify' => false,   // AI Factory на HTTP (внутренняя сеть)
        ]
    );
} );
