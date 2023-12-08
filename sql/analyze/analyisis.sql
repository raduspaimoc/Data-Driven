---- Bookings by agent
SELECT du.username as agent_name, COUNT(fb.booking_id) as num_bookings
FROM dbo.dim_users du
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
GROUP BY du.username;

---- Bookings by company
SELECT dc.company_name, COUNT(fb.booking_id) as num_bookings
FROM dbo.dim_companies dc
LEFT JOIN dbo.dim_users du ON dc.company_id = du.company_id
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
GROUP BY dc.company_name;

---- Meal Preferences
SELECT
    meal,
    meal_package,
	COUNT(booking_id) as num_bookings
FROM (
    SELECT
        dm.meal,
        fb.booking_id,
        CASE
            WHEN dm.meal IN ('Undefined', 'SC') THEN 'No meal package'
            WHEN dm.meal = 'BB' THEN 'Bed & Breakfast'
            WHEN dm.meal = 'HB' THEN 'Half board'
            WHEN dm.meal = 'FB' THEN 'Full board'
            ELSE 'Other'
        END as meal_package
    FROM dbo.dim_meals dm
    LEFT JOIN dbo.fact_bookings fb ON dm.meal_id = fb.meal_id
) AS subquery
GROUP BY meal, meal_package;

---- Canceled bookings by country
SELECT fb.country, COUNT(fb.booking_id) as num_cancelations
FROM dbo.fact_bookings fb
WHERE fb.is_canceled = 1
GROUP BY fb.country;

---- AVG stay nights by user
SELECT du.username, AVG(fb.stays_in_weekend_nights + fb.stays_in_week_nights) as avg_stay
FROM dbo.dim_users du
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
GROUP BY du.username;


---- Bookings per season
SELECT
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END as season,
	COUNT(fb.booking_id) as num_bookings
FROM dbo.dim_users du
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
GROUP BY
         CASE
            WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
            WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
            WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
            WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
            ELSE 'Otro'
        END;

----- Bookings and stays
SELECT
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END as season,
    COUNT(fb.booking_id) as num_bookings,
    SUM(fb.stays_in_weekend_nights + fb.stays_in_week_nights) as total_stay,
    AVG(fb.stays_in_weekend_nights + fb.stays_in_week_nights) as avg_stay
FROM dbo.dim_users du
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
GROUP BY
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END;

----- Booking stays and meals per seasons
SELECT
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END as season,
    COUNT(fb.booking_id) as num_bookings,
    SUM(fb.stays_in_weekend_nights + fb.stays_in_week_nights) as total_stay,
    AVG(fb.stays_in_weekend_nights + fb.stays_in_week_nights) as avg_stay,
    COUNT(CASE WHEN dm.meal = 'Undefined' THEN 1 END) as num_undefined_meal,
    COUNT(CASE WHEN dm.meal = 'BB' THEN 1 END) as num_bed_breakfast,
    COUNT(CASE WHEN dm.meal = 'HB' THEN 1 END) as num_half_board,
    COUNT(CASE WHEN dm.meal = 'FB' THEN 1 END) as num_full_board
FROM dbo.dim_users du
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
LEFT JOIN dbo.dim_meals dm ON fb.meal_id = dm.meal_id
GROUP BY
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END;


----- Booking stays, meals and cancelations per seasons
SELECT
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END as season,
    COUNT(fb.booking_id) as num_bookings,
    COUNT(CASE WHEN fb.is_canceled = 1 THEN 1 END) as num_cancelled_bookings,
    ROUND(CAST(COUNT(CASE WHEN fb.is_canceled = 1 THEN 1 END) AS FLOAT) / COUNT(fb.booking_id) * 100, 2) as cancelled_percentage,
    SUM(fb.stays_in_weekend_nights + fb.stays_in_week_nights) as total_stay_in_days,
    AVG(fb.stays_in_week_nights) as avg_stay_in_week,
    AVG(fb.stays_in_week_nights + fb.stays_in_weekend_nights) as avg_stay_total,
    COUNT(CASE WHEN dm.meal = 'Undefined' THEN 1 END) as num_undefined_meal,
    COUNT(CASE WHEN dm.meal = 'BB' THEN 1 END) as num_bed_breakfast,
    COUNT(CASE WHEN dm.meal = 'HB' THEN 1 END) as num_half_board,
    COUNT(CASE WHEN dm.meal = 'FB' THEN 1 END) as num_full_board
FROM dbo.dim_users du
LEFT JOIN dbo.fact_bookings fb ON du.id = fb.agent_id
LEFT JOIN dbo.dim_meals dm ON fb.meal_id = dm.meal_id
GROUP BY
    CASE
        WHEN MONTH(fb.reservation_status_date) IN (12, 1, 2) THEN 'Invierno'   -- Diciembre, Enero, Febrero
        WHEN MONTH(fb.reservation_status_date) IN (3, 4, 5) THEN 'Primavera'  -- Marzo, Abril, Mayo
        WHEN MONTH(fb.reservation_status_date) IN (6, 7, 8) THEN 'Verano'     -- Junio, Julio, Agosto
        WHEN MONTH(fb.reservation_status_date) IN (9, 10, 11) THEN 'Otoño'    -- Septiembre, Octubre, Noviembre
        ELSE 'Otro'
    END;