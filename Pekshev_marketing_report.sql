
WITH fb_ggl_report AS 
	(WITH all_fb_ggl AS 
		(WITH fb_ggl AS (SELECT
		ad_date, 
		url_parameters,
		COALESCE(spend,0) AS spend,
		COALESCE(impressions,0) AS impressions,
		COALESCE(reach,0) AS reach,
		COALESCE(clicks,0) AS clicks,
		COALESCE(leads,0) AS leads,
		COALESCE(value,0) AS value
		FROM facebook_ads_basic_daily fabd
		UNION all
		SELECT
		ad_date, 
		url_parameters,
		COALESCE(spend,0) AS spend,
		COALESCE(impressions,0) AS impressions,
		COALESCE(reach,0) AS reach,
		COALESCE(clicks,0) AS clicks,
		COALESCE(leads,0) AS leads,
		COALESCE(value,0) AS value
		FROM google_ads_basic_daily gabd)
	SELECT
	date_trunc ('month', ad_date) AS ad_month,
	CASE WHEN lower(substring(url_parameters, 'utm_campaign=([^&]+)')) = 'nan' THEN NULL
	ELSE lower(substring(url_parameters, 'utm_campaign=([^\&]+)')) END AS utm_campaign,
	sum(spend) AS total_spend,
	sum(impressions) AS total_impressions,
	sum(clicks) AS total_clicks,
	sum(value) AS total_revenue,
	CASE 
		WHEN sum(impressions::numeric) >0 
		THEN round((sum(clicks::numeric)/sum(impressions::numeric)*100),3) 
		ELSE 0 END AS ctr,
	CASE 
		WHEN sum(clicks::numeric) >0 
		THEN round((sum(spend::numeric)/sum(clicks::numeric)),2) 
		ELSE 0 END AS cpc,
	CASE 
		WHEN sum(impressions::numeric) >0 
		THEN round(((sum(spend::numeric)/sum(impressions::numeric))*1000),2) 
		ELSE 0 END AS cpm,
	CASE 
		WHEN sum(spend::numeric) >0 
		THEN round(((sum(value::numeric)-sum(spend::numeric))/sum(spend::numeric)*100),2) 
		ELSE 0 END AS romi
	FROM fb_ggl
	GROUP BY ad_month, utm_campaign)
SELECT 
	ad_month, utm_campaign, total_spend, total_impressions, total_clicks, total_revenue, ctr, cpc, cpm, romi,
	LAG (ctr,1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ctr,
	LAG (cpm,1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_cpm,
	LAG (romi,1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_romi
FROM all_fb_ggl)
SELECT ad_month, utm_campaign, total_spend, total_impressions, total_clicks, total_revenue, ctr, cpc, cpm, romi,
	round((((ctr-prev_ctr)/prev_ctr)*100),2) AS ctr_diff,
	round((((cpm-prev_cpm)/prev_cpm)*100),2) AS cpm_diff,
	round((((romi-prev_romi)/prev_romi)*100),2) AS romi_diff
FROM fb_ggl_report

	