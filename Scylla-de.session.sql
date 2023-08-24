CREATE TABLE IF NOT EXISTS devices.events (
    panel_id text,
    per_capita_income text,
    age_group text,
    gender text,
    panel_type text,

    maid text,
    event_date text,

    brands Set<text>,
    pois_categories Set<text>,
    subcategories Set<text>,


    PRIMARY KEY (panel_id, per_capita_income, age_group, gender, panel_type) 
);
-- drop table de.panels

-- create table de.panels (
--     id Text primary key,
--     city Text,
--     uf Text,
--     maids_count bigint
-- )


-- drop table devices.events;

