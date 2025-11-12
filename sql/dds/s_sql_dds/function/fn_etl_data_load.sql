create or replace function s_sql_dds.fn_etl_data_load(start_date date, end_date date)
returns void language plpgsql as
$$
declare
    countries text[] := array[
        'afghanistan','aland islands','albania','algeria','american samoa','andorra','angola','anguilla','antarctica','antigua and barbuda',
        'argentina','armenia','aruba','australia','austria','azerbaijan','bahamas','bahrain','bangladesh','barbados','belarus','belgium',
        'belize','benin','bermuda','bhutan','bolivia','bosnia and herzegovina','botswana','bouvet island','brazil','british indian ocean territory',
        'brunei darussalam','bulgaria','burkina faso','burundi','cabo verde','cambodia','cameroon','canada','cayman islands','central african republic',
        'chad','chile','china','christmas island','cocos (keeling) islands','colombia','comoros','congo','cook islands','costa rica','cote d''ivoire',
        'croatia','cuba','curacao','cyprus','czech republic','denmark','djibouti','dominica','dominican republic','ecuador','egypt','el salvador',
        'equatorial guinea','eritrea','estonia','eswatini','ethiopia','falkland islands (malvinas)','faroe islands','fiji','finland','france','french guiana',
        'french polynesia','gabon','gambia','georgia','germany','ghana','gibraltar','greece','greenland','grenada','guadeloupe','guam','guatemala',
        'guernsey','guinea','guinea-bissau','guyana','haiti','heard island and mcdonald islands','honduras','hong kong','hungary','iceland','india','indonesia',
        'iran','iraq','ireland','isle of man','israel','italy','jamaica','japan','jersey','jordan','kazakhstan','kenya','kiribati','kosovo','kuwait','kyrgyzstan',
        'laos','latvia','lebanon','lesotho','liberia','libya','liechtenstein','lithuania','luxembourg','macao','madagascar','malawi','malaysia','maldives',
        'mali','malta','marshall islands','martinique','mauritania','mauritius','mayotte','mexico','micronesia','moldova','monaco','mongolia','montenegro',
        'montserrat','morocco','mozambique','myanmar','namibia','nauru','nepal','netherlands','new caledonia','new zealand','nicaragua','niger','nigeria',
        'niue','norfolk island','north macedonia','northern mariana islands','norway','oman','pakistan','palau','panama','papua new guinea','paraguay','peru',
        'philippines','pitcairn','poland','portugal','puerto rico','qatar','reunion','romania','russia','rwanda','saint barthelemy','saint helena, ascension and tristan da cunha',
        'saint kitts and nevis','saint lucia','saint martin','saint pierre and miquelon','saint vincent and the grenadines','samoa','san marino','sao tome and principe',
        'saudi arabia','senegal','serbia','seychelles','sierra leone','singapore','sint maarten','slovakia','slovenia','solomon islands','somalia','south africa',
        'south georgia and the south sandwich islands','south korea','south sudan','spain','sri lanka','sudan','suriname','svalbard and jan mayen','sweden','switzerland',
        'syria','taiwan','tajikistan','tanzania','thailand','timor-leste','togo','tokelau','tonga','trinidad and tobago','tunisia','turkey','turkmenistan','turks and caicos islands',
        'tuvalu','uganda','ukraine','united arab emirates','united kingdom','united states','united states minor outlying islands','uruguay','uzbekistan','vanuatu','vatican city','venezuela','vietnam','wallis and futuna','western sahara','yemen','zambia','zimbabwe'
    ];
begin
    with prepared as (
        select
            coalesce(nullif(trim(t.id), ''), gen_random_uuid()::text)::text as id_val,
            coalesce(nullif(trim(t.name), ''), 'unknown')::text as name_val,
            case
                when t.age is null then null
                when (t.age::integer) between 0 and 120 then t.age::integer
                else null
            end as age_val,
            nullif(trim(t.category), '')::text as category_val,
            case
                when t.value is null then null
                when (t.value::numeric) < 0 then null
                else t.value::numeric
            end as value_val,
            case
                when t.country is not null and trim(t.country) <> '' and lower(trim(t.country)) = any(countries)
                    then trim(t.country)
                else 'unknown'
            end as country_val,
            coalesce(nullif(trim(t.city), ''), 'unknown')::text as city_val,
            coalesce((t.signup_date::date), current_date) as signup_date_val,
            coalesce(
                (case
                    when t.email is not null
                         and position('@' in t.email) > 1
                         and position('.' in substring(t.email from position('@' in t.email) + 1)) > 1
                    then lower(trim(t.email))
                    else null
                end),
                'unknown'
            ) as email_val
        from s_sql_dds.t_sql_source_unstructured t
        where coalesce((t.signup_date::date), current_date) between start_date and end_date
    ), dedup as (
        -- выбираем одну строку на id (последнюю по signup_date)
        select distinct on (id_val)
            id_val, name_val, age_val, category_val, value_val, country_val, city_val, signup_date_val, email_val
        from prepared
        order by id_val, signup_date_val desc
    )
    insert into s_sql_dds.t_sql_source_structured (
        id, name, age, category, value, country, city, signup_date, email
    )
    select id_val, name_val, age_val, category_val, value_val, country_val, city_val, signup_date_val, email_val
    from dedup
    on conflict (id) do update set
        name = excluded.name,
        age = excluded.age,
        category = excluded.category,
        value = excluded.value,
        country = excluded.country,
        city = excluded.city,
        signup_date = excluded.signup_date,
        email = excluded.email;
end;
$$;
