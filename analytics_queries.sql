-- Analytics Queries for Shopper Behavior Analysis
-- Demonstrates SQL skills and business understanding

-- 1. Daily Revenue and Conversion Metrics
SELECT 
    date,
    COUNT(DISTINCT user_id) as daily_active_users,
    SUM(revenue) as total_revenue,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as total_purchases,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as total_page_views,
    ROUND(
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)::FLOAT / 
        NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0) * 100, 2
    ) as conversion_rate_pct
FROM analytics.events
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date
ORDER BY date DESC;

-- 2. Top Performing Products by Category
WITH product_performance AS (
    SELECT 
        category,
        product_id,
        SUM(revenue) as total_revenue,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
        COUNT(*) as total_interactions,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) as rank_in_category
    FROM analytics.events
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY category, product_id
)
SELECT 
    category,
    product_id,
    total_revenue,
    purchases,
    total_interactions,
    ROUND(purchases::FLOAT / total_interactions * 100, 2) as conversion_rate
FROM product_performance
WHERE rank_in_category <= 5
ORDER BY category, total_revenue DESC;

-- 3. User Segmentation by Purchase Behavior
WITH user_segments AS (
    SELECT 
        user_id,
        COUNT(DISTINCT session_id) as total_sessions,
        SUM(revenue) as lifetime_value,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as total_purchases,
        MAX(date) as last_activity_date,
        CASE 
            WHEN SUM(revenue) >= 1000 THEN 'High Value'
            WHEN SUM(revenue) >= 500 THEN 'Medium Value'
            WHEN SUM(revenue) > 0 THEN 'Low Value'
            ELSE 'Browser'
        END as customer_segment
    FROM analytics.events
    GROUP BY user_id
)
SELECT 
    customer_segment,
    COUNT(*) as user_count,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(total_purchases) as avg_purchases,
    AVG(total_sessions) as avg_sessions
FROM user_segments
GROUP BY customer_segment
ORDER BY avg_lifetime_value DESC;

-- 4. Ad Campaign Effectiveness Analysis
SELECT 
    ad_campaign_id,
    COUNT(DISTINCT user_id) as unique_users_reached,
    COUNT(*) as total_ad_interactions,
    SUM(revenue) as attributed_revenue,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as conversions,
    ROUND(
        SUM(revenue) / NULLIF(COUNT(*), 0), 2
    ) as revenue_per_interaction,
    ROUND(
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)::FLOAT / 
        NULLIF(COUNT(*), 0) * 100, 2
    ) as conversion_rate_pct
FROM analytics.events
WHERE ad_campaign_id IS NOT NULL
    AND date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY ad_campaign_id
HAVING COUNT(*) >= 100  -- Filter campaigns with significant volume
ORDER BY attributed_revenue DESC;

-- 5. Device and Location Performance
SELECT 
    device_type,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(revenue) as avg_revenue_per_user,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as page_views,
    ROUND(
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)::FLOAT / 
        NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0) * 100, 2
    ) as conversion_rate
FROM analytics.events
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY device_type
ORDER BY avg_revenue_per_user DESC;

-- 6. Hourly Traffic and Revenue Patterns
SELECT 
    EXTRACT(hour FROM timestamp) as hour_of_day,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(revenue) as hourly_revenue,
    AVG(revenue) as avg_revenue_per_event
FROM analytics.events
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY EXTRACT(hour FROM timestamp)
ORDER BY hour_of_day;

-- 7. Customer Journey Analysis
WITH session_funnel AS (
    SELECT 
        session_id,
        user_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as has_page_view,
        MAX(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) as has_search,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as has_add_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
        SUM(revenue) as session_revenue
    FROM analytics.events
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY session_id, user_id
)
SELECT 
    'Page View' as funnel_step,
    SUM(has_page_view) as sessions_count,
    ROUND(SUM(has_page_view)::FLOAT / COUNT(*) * 100, 2) as percentage
FROM session_funnel
UNION ALL
SELECT 
    'Search' as funnel_step,
    SUM(has_search) as sessions_count,
    ROUND(SUM(has_search)::FLOAT / COUNT(*) * 100, 2) as percentage
FROM session_funnel
UNION ALL
SELECT 
    'Add to Cart' as funnel_step,
    SUM(has_add_to_cart) as sessions_count,
    ROUND(SUM(has_add_to_cart)::FLOAT / COUNT(*) * 100, 2) as percentage
FROM session_funnel
UNION ALL
SELECT 
    'Purchase' as funnel_step,
    SUM(has_purchase) as sessions_count,
    ROUND(SUM(has_purchase)::FLOAT / COUNT(*) * 100, 2) as percentage
FROM session_funnel;