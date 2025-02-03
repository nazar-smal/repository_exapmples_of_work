WITH plan_mapping AS (
    SELECT DISTINCT
        associated_plan,
        plan_name,
        billing_interval
    FROM mappings.hootsuite_plans
    WHERE include_in_mrr = TRUE
)
,all_paying_users AS (
    SELECT
        DISTINCT ip.member_id,
        DATE_TRUNC('day',ip.payment_date) AS payment_date,
        pm.plan_name,
        billing_interval,
        SUM(amount) AS amount,
        SUM(amount_in_usd) AS amount_in_usd
    FROM self_serve.invoice_payments ip
    INNER JOIN plan_mapping pm
        ON pm.associated_plan = ip.associated_plan
    LEFT JOIN mappings.hootsuite_service_numbers sn
        ON sn.service_no = ip.service_no
    WHERE sn.exclude_from_mrr = FALSE
        AND DATE_TRUNC('day', ip.payment_date) <= current_date
    GROUP BY ip.member_id, DATE_TRUNC('day',ip.payment_date), pm.plan_name, pm.billing_interval
    HAVING SUM(amount) > 0 AND SUM(amount_in_usd) > 0
),
monthly_paying_users AS (
    SELECT
        DATE_TRUNC('month',pu.payment_date)::date AS payment_date,
        pu.member_id,
        MAX(pu.amount_in_usd) AS max_payment,
        MAX(pu.payment_number) AS max_payment_no
    FROM (
        SELECT
            payment_date,
            member_id,
            amount_in_usd,
            ROW_NUMBER() OVER (PARTITION BY member_id, DATE_TRUNC('month', payment_date) ORDER BY amount_in_usd ASC, payment_date ASC) AS payment_number
        FROM all_paying_users) pu
    GROUP BY DATE_TRUNC('month',pu.payment_date), pu.member_id
),
paying_users_unique AS (
    SELECT
        ap.payment_date::date AS date,
        mp.member_id,
        ap.plan_name,
        ap.billing_interval
    FROM monthly_paying_users mp
    INNER JOIN (
        SELECT
            payment_date,
            member_id,
            amount_in_usd,
            plan_name,
            billing_interval,
            ROW_NUMBER() OVER (PARTITION BY member_id, DATE_TRUNC('month', payment_date) ORDER BY amount_in_usd ASC, payment_date ASC) AS payment_number
        FROM all_paying_users
    ) ap
        ON (mp.member_id = ap.member_id AND mp.payment_date = DATE_TRUNC('month', ap.payment_date) AND mp.max_payment_no = ap.payment_number AND mp.max_payment = ap.amount_in_usd)
)
,customer_segments AS (
    SELECT
        u.date,
        u.member_id,
        u.plan_name,
        u.billing_interval,
        c.market,
        c.trial_type,
        c.trial_length,
        c.acquisition_source,
        c.signup_source
    FROM paying_users_unique u
    INNER JOIN self_serve.customers c
        ON u.member_id = c.member_id
),
segments_shell AS (
    SELECT
        DISTINCT date,
        market,
        plan,
        billing_interval,
        trial_type,
        trial_length,
        acquisition_source,
        signup_source
    FROM self_serve.segments_shell
),
daily_paying_users AS (
    SELECT
        date_trunc('day',date) AS date,
        COUNT(DISTINCT member_id) AS num_paying_daily,
        market,
        plan_name AS plan,
        billing_interval,
        trial_type,
        trial_length,
        acquisition_source,
        signup_source
    FROM customer_segments

    GROUP BY date_trunc('day',date), market, plan_name, billing_interval, trial_type, trial_length, acquisition_source, signup_source
),
paying_users_segmented AS (
    SELECT
        s.date,
        s.market,
        s.plan,
        s.billing_interval,
        s.trial_type,
        s.trial_length,
        s.acquisition_source,
        s.signup_source,
        num_paying_daily
    FROM segments_shell s
    LEFT JOIN daily_paying_users d
        ON s.date = d.date
        AND s.market = d.market
        AND s.plan = d.plan
        AND s.billing_interval = d.billing_interval
        AND s.trial_type = d.trial_type
        AND s.trial_length = d.trial_length
        AND s.acquisition_source = d.acquisition_source
        AND s.signup_source = d.signup_source
),
num_paying AS (
    SELECT
        date::date,
        market,
        plan,
        billing_interval,
        trial_type,
        trial_length,
        acquisition_source,
        signup_source,
        CASE WHEN (DATE_PART('month',date) IN (1,8)
            OR (p.date != LAST_DAY(date) AND DATE_PART('month',date) IN (2,4,6,9,11))
            OR (p.date = LAST_DAY(date) AND DATE_PART('month',date) IN (3,5,7,10,12)))
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)

            WHEN ((p.date != LAST_DAY(date) AND DATE_PART('month',date) IN (5,7,10,12))
            OR (p.date = LAST_DAY(date) AND DATE_PART('month',date) IN (4,6,9,11)))
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)

            WHEN (((DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 28 AND DATE_PART('day',date) < 28)) OR (date = LAST_DAY(date) AND DATE_PART('day',LAST_DAY(date)) = 28))
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)

            WHEN (((DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 29 AND DATE_PART('day',date) < 29)) OR (date = LAST_DAY(date) AND DATE_PART('day',LAST_DAY(date)) = 29))
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 28 PRECEDING AND CURRENT ROW)

            WHEN (DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 28 AND DATE_PART('day',date) = 28)
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING)

            WHEN (DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 28 AND DATE_PART('day',date) = 29)
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 29 PRECEDING AND 2 PRECEDING)

            WHEN (DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 29 AND DATE_PART('day',date) = 29)
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING)

            WHEN (DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 28 AND DATE_PART('day',date) = 30)
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 30 PRECEDING AND 3 PRECEDING)

            WHEN (DATE_PART('month',date) = 3 AND DATE_PART('day',LAST_DAY(DATE_ADD('month',-1,date))) = 29 AND DATE_PART('day',date) = 30)
            THEN SUM(SUM(num_paying_daily)) OVER (PARTITION BY market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source ORDER BY date
                                        ROWS BETWEEN 30 PRECEDING AND 2 PRECEDING)
        END AS num_paying

    FROM paying_users_segmented p
    GROUP BY date, market, plan, billing_interval, trial_type, trial_length, acquisition_source, signup_source
)
SELECT
    date,
    market,
    plan,
    billing_interval,
    trial_type,
    trial_length,
    acquisition_source,
    signup_source,
    num_paying
FROM num_paying
WHERE num_paying IS NOT NULL