select 
    policies.number as policy_number 
    ,policies.id as policy_id
    ,transaction_id as transaction_id
    ,insurance_number as insurance_policy_number
    ,insurance_old_number as insurance_old_policy_number
    ,products.name as product_name
    ,products_policies.product_price*bookings.room as GWP
    ,products_policies.product_price*bookings.room*bookings.days as total_GWP
    ,'' as NWP
    ,(products_policies.product_price*bookings.room*bookings.days)*0.1 as Net_Revenue
    ,insurance_partners.name as insurance_name
    ,partners.name as partner_name
    ,booker_name 
    ,user_policies.user_id as user_id
    ,booker_phone
    ,bookings.country_id as country_id
    ,oyo_countries.name as country
    ,bookings.currency as currency
    ,policies.code as booking_code
    ,bookings.amount as booking_price
    ,bookings.days as days
    ,bookings.room as room
    ,json_extract_path_text(bookings.details, 'rooms','count') as total_people
    ,json_extract_path_text(bookings.details, 'rooms','passengerCount','adult') as total_adult
    ,json_extract_path_text(bookings.details, 'totalBookingPrice') as total_booking_price
    ,policies.booking_date as purchase_time
    ,policies.created_at as policies_created_time
    --,DATE_PART('day', policies.booking_date - bookings.created_at) as purchase_to_booking
    ,transaction_histories.payment_type as payment_type
    ,transaction_histories.created_at as payment_time
    ,json_extract_path_text(bookings.details, 'category') as category
    ,json_extract_path_text(bookings.details, 'city') as city
    ,bookings.sales_channel as sales_channel
    ,bookings.sales_channel_name as sales_channel_name
    ,policies.status as policy_status
    ,endorsments.updated_at as last_endorse_time
    ,endorsments.endorsment_type as endorsment_type
    ,endorsments.endorsment_time as endorse_started_time
    from 
    accommodation_service_production.policies
    left join accommodation_service_production.bookings on bookings.id = booking_id
    left join accommodation_service_production.products_policies on products_policies.policy_id = policies.id
    left join accommodation_service_production.user_policies on user_policies.id = policies.number
    left join accommodation_service_production.products on products.id = products_policies.product_id
    left join user_service_production.partners on policies.partner_id = partners.id
    left join user_service_production.insurance_partners on insurance_partners.id = policies.insurance_partner_id
    left join 
    (select * from 
        (select 
            *, Row_number() over (partition by partner_transaction_id,endorsment_type order by updated_at desc) as rn
        from accommodation_integration_production.endorsments)endorse where rn =1)endorsments
            on policies.transaction_id = endorsments.partner_transaction_id
    left join accommodation_integration_production.transaction_histories on policies.transaction_id = transaction_histories.partner_transaction_id
    left join datamart.oyo_countries on oyo_countries.code = bookings.country_id 
    where 
    policies.transaction_id not like '%TEST%'
    and 
    policies.transaction_id not like '%TES%'
    and 
    policies.transaction_id not like '%OYO-RELEASE%'