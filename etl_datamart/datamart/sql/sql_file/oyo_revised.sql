select 
policies.number as policy_number 
,transaction_id as transaction_id
,NULL AS RefNo
,policies.booking_date as policy_purchase_time
,policies.created_at as policy_activation_time
,bookings.start_date as policy_started_time
,bookings.end_date as policy_expired_time
,policies.status as policy_status
,policies.partner_id as partner_id
,partners.name as partner_name
,'Partnership API' as channel
,'Travel' as client_category
,'Accomodation' as product_Category
,'Accomodation' as product_group
,policies.insurance_partner_id as insurance_ID
,insurance_partners.name as insurance_name
,covered_users.id as user_id
,policies.id as policy_id
,null as invoice_id
,null as invoice_number
,policies.code as partner_transaction_id
,transaction_histories.payment_type as payment_type
,null as payment_bank
,bookings.currency as currency
,oyo_countries.name as country
,products_policies.product_price*bookings.room*bookings.days as gwp_local
,CASE WHEN country_id = 96 THEN (products_policies.product_price*bookings.room*bookings.days)/cu.usd::float
	  WHEN country_id = 217 THEN (products_policies.product_price*bookings.room*bookings.days)* 0.000043::float
	  WHEN country_id = 160 then ((products_policies.product_price*bookings.room*bookings.days)*php)/cu.usd::float
	  WHEN country_id = 8 then ((products_policies.product_price*bookings.room*bookings.days)*thb)/cu.usd::float
	  ELSE 0 END AS gwp_usd
,0 as nwp_local
,0 as nwp_usd
,0 as total_commission_local
,0 as total_commission_usd
,0 as total_commission_to_partner_local
,0 as total_commission_to_partner_usd
,0 as qoala_earned_local
,0 as qoala_earned_usd
,(products_policies.product_price*bookings.room*bookings.days)*0.1 as nett_revenue_local
,CASE WHEN country_id = 96 THEN ((products_policies.product_price*bookings.room*bookings.days)*0.1)/cu.usd::float
	  WHEN country_id = 217 THEN ((products_policies.product_price*bookings.room*bookings.days)*0.1)* 0.000043::float
	  WHEN country_id = 160 then (((products_policies.product_price*bookings.room*bookings.days)*0.1)*php)/cu.usd::float
	  WHEN country_id = 8 then (((products_policies.product_price*bookings.room*bookings.days)*0.1)*thb)/cu.usd::float
	  ELSE 0 END AS nett_revenue_usd
,policies.created_at as policy_issued_time
,policies.booking_date as insurance_recon_time
,policies.booking_date as partner_recon_time
,'UTC' as timezone
,'true' as MJP
, 1 as number_of_segment
,products.name as product_name
,'API' as integration_type
,insurance_number as insurance_policy_number
,insurance_old_number as insurance_old_policy_number
,booker_name 
,booker_phone
,policies.code as booking_code
,bookings.amount as booking_price
,bookings.days as days
,bookings.room as room
,json_extract_path_text(bookings.details, 'rooms','count') as total_people
,json_extract_path_text(bookings.details, 'rooms','passengerCount','adult') as total_adult
,json_extract_path_text(bookings.details, 'totalBookingPrice') as total_booking_price
,transaction_histories.created_at as payment_time
,json_extract_path_text(bookings.details, 'category') as category
,json_extract_path_text(bookings.details, 'city') as city
,bookings.sales_channel as sales_channel
,bookings.sales_channel_name as sales_channel_name
,endorsments.updated_at as last_endorse_time
,endorsments.endorsment_type as endorsment_type
,endorsments.endorsment_time as endorse_started_time
,user_policies.user_id as qapp_user_id
from 
accommodation_service_production.policies
left join accommodation_service_production.bookings on bookings.id = booking_id
left join accommodation_service_production.products_policies on products_policies.policy_id = policies.id
LEFT JOIN accommodation_service_production.covered_users ON covered_users.policy_id = policies.id
left join accommodation_service_production.user_policies on user_policies.policy_number = policies.number
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
LEFT JOIN analytics_schema.idr_to_foreign_curr cu ON date(policies.booking_date) = cu.trx_date
where 
policies.transaction_id not like '%TEST%'
and 
policies.transaction_id not like '%TES%'
and 
policies.transaction_id not like '%OYO-RELEASE%'
