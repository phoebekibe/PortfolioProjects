/*
Maji Ndogo Data Cleaning & Data Exploration

Skills used: Aggregate Functions, Ranking Functions, String functions, Joins, CTE's, Temp Tables, Creating Views, Converting Data Types
*/

-- DATA CLEANING
-- Looking at sources described as Clean
SELECT * 
FROM md_water_services.well_pollution
WHERE description LIKE 'Clean_%';

-- Create a copy of the data in a temp table before making any corrections
CREATE TABLE md_water_services.well_pollution_copy AS (
	SELECT *
	FROM md_water_services.well_pollution
);

SET SQL_SAFE_UPDATES = 0;

-- Case 1a: Update descriptions that mistakenly mention `Clean Bacteria: E. coli` to `Bacteria: E. coli`
UPDATE md_water_services.well_pollution
SET description = 'Bacteria: E. coli'
WHERE description = 'Clean Bacteria: E. coli';

-- Case 1b: Update the descriptions that mistakenly mention `Clean Bacteria: Giardia Lamblia` to `Bacteria: Giardia Lamblia
UPDATE well_pollution
SET description = 'Bacteria: Giardia Lamblia'
WHERE description = 'Clean Bacteria: Giardia Lamblia';

-- Case 2: Update the `result` to `Contaminated: Biological` where `biological` is greater than 0.01 plus current results is `Clean`
UPDATE well_pollution
SET results = 'Contaminated: Biological'
WHERE biological > 0.01 AND results = 'Clean';

-- Use a test query here to confirm the errors are fixed , then we can drop the temp table
SELECT * 
FROM md_water_services.well_pollution
WHERE description LIKE "Clean_%"
	OR (results = "Clean" AND biological > 0.01);
    
DROP TABLE md_water_services.well_pollution_copy;

-- Update email of employees
UPDATE 	employee
SET email= CONCAT(LOWER(REPLACE(employee_name, ' ', '.')), '@ndogowater.gov');

-- Update employees phone numbers by trimming leading spaces
SELECT LENGTH(phone_number)
FROM employee;

SELECT 
	TRIM(phone_number) AS new_phone_number,
    LENGTH(TRIM(phone_number))
FROM employee;

UPDATE employee
SET phone_number = TRIM(phone_number);



-- DATA EXPLORATION

-- Number of employees per town and province
SELECT
	town_name,province_name,
    COUNT(assigned_employee_id) AS num_of_employees
FROM
	Employee
GROUP BY 
	town_name,province_name;
    
-- Number of visits made per employee
SELECT
	assigned_employee_id,
    COUNT(visit_count) AS num_of_visits
FROM visits
GROUP BY assigned_employee_id
ORDER BY COUNT(visit_count) DESC;

-- Employees with the least number of visits made
SELECT
	assigned_employee_id, employee_name, email, phone_number
FROM employee
WHERE assigned_employee_id IN ("22","20");

-- Total avg queue time per location
SELECT 
    location_id,
    time_in_queue,
    AVG(time_in_queue) OVER (PARTITION BY location_id ORDER BY visit_count) AS total_avg_queue_time
FROM  visits
WHERE visit_count > 1 -- Only shared taps were visited more than once
ORDER BY location_id, time_of_record;

-- Number of records per town
SELECT
	province_name, town_name,
    COUNT(location_id) AS records_per_town
FROM location
GROUP BY province_name, town_name
ORDER BY COUNT(location_id) DESC;

-- Total number of water sources in Urban and Rural area
SELECT
	location_type,
	COUNT(location_id) AS num_sources
FROM location
GROUP BY location_type;

-- Total number of people served by water sources
SELECT 
	SUM(number_of_people_served) AS total_no_surveyed,
    COUNT(source_id) AS total_no_sources
FROM md_water_services.water_source;

-- Total number of people served by taps
SELECT
	SUM(number_of_people_served) AS population_served
FROM water_source
WHERE type_of_water_source LIKE "%tap%"
ORDER BY population_served;

-- Percentage of people served per type of water source
SELECT
	type_of_water_source,
	COUNT(source_id) AS num_of_sources,
    ROUND(AVG(number_of_people_served)) AS avg_people_per_source,
    ROUND(SUM(number_of_people_served)/27628140 * 100) AS percentage_people_served_per_source
FROM water_source
GROUP BY type_of_water_source;

-- Ranking the types of water sources according to which is most used
SELECT
	type_of_water_source,
	COUNT(source_id) AS num_of_sources,
    ROUND(AVG(number_of_people_served)) AS avg_people_per_source,
    ROUND(SUM(number_of_people_served)/27628140 * 100) AS percentage_people_served_per_source,
    RANK() OVER (ORDER BY ROUND(SUM(number_of_people_served)/27628140 * 100) DESC) AS rank_by_population
FROM water_source
WHERE type_of_water_source IN ("shared_tap","well","tap_in_home_broken","river")
GROUP BY type_of_water_source;

-- Ranking the sources according to number of people served to determine the order of improving the sources 
SELECT
	source_id, type_of_water_source, number_of_people_served,
    RANK() OVER (PARTITION BY type_of_water_source ORDER BY number_of_people_served DESC) AS priority_rank,
    ROW_NUMBER() OVER (PARTITION BY type_of_water_source ORDER BY number_of_people_served DESC) AS row_number_rank
FROM 
	water_source
WHERE 
	type_of_water_source IN ("shared_tap","well","tap_in_home_broken","river");
    
-- Total duration of surveys done on water sources
SELECT
    DATEDIFF(MAX(time_of_record), MIN(time_of_record) ) AS Survey_duration,
    TIMESTAMPDIFF(day, MIN(time_of_record), MAX(time_of_record)) AS Survey_duration_TIMESTAMP
FROM
	visits;

-- Avg queue time on different days of the week
SELECT
    DAYNAME(time_of_record) AS day_of_week,
    AVG(time_in_queue) AS avg_queue_time
FROM
	visits
GROUP BY DAYNAME(time_of_record);

-- JOIN location, water_source and well_pollution tables to visits table.
SELECT
	location.province_name, 
    location.town_name,
    ws.type_of_water_source,
    location.location_type,
    ws.number_of_people_served,
    visits.time_in_queue,
    well_pollution.results
FROM visits
LEFT JOIN well_pollution ON well_pollution.source_id = visits.source_id
INNER JOIN location ON location.location_id = visits.location_id
INNER JOIN water_source AS ws ON ws.source_id = visits.source_id
WHERE visits.visit_count = 1;

-- Create view to assemble the joined tables as one to simplify analysis
CREATE VIEW combined_analysis_table AS
SELECT
	location.province_name, 
    location.town_name,
    ws.type_of_water_source AS source_type,
    location.location_type,
    ws.number_of_people_served AS people_served,
    visits.time_in_queue,
    well_pollution.results
FROM visits
LEFT JOIN well_pollution ON well_pollution.source_id = visits.source_id
INNER JOIN location ON location.location_id = visits.location_id
INNER JOIN water_source AS ws ON ws.source_id = visits.source_id
WHERE visits.visit_count = 1;

-- The CTE below calculates the percentage of people served by each type of water source , grouped by province then by town
-- Since there are two Harare towns, we have to group by province_name and town_name
WITH town_totals AS (
	SELECT province_name, town_name, SUM(people_served) AS total_ppl_serv
	FROM combined_analysis_table
	GROUP BY province_name,town_name
)
SELECT
	ct.province_name,
	ct.town_name,
		ROUND((SUM(CASE WHEN source_type = 'river'
			THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS river,
		ROUND((SUM(CASE WHEN source_type = 'shared_tap'
			THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS shared_tap,
		ROUND((SUM(CASE WHEN source_type = 'tap_in_home'
			THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home,
		ROUND((SUM(CASE WHEN source_type = 'tap_in_home_broken'
			THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home_broken,
		ROUND((SUM(CASE WHEN source_type = 'well' 
			THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS well
	FROM
	combined_analysis_table ct
JOIN -- Since the town names are not unique, we have to join on a composite key
	town_totals tt ON ct.province_name = tt.province_name AND ct.town_name = tt.town_name
GROUP BY -- We group by province first, then by town.
	ct.province_name,
	ct.town_name
ORDER BY
	ct.town_name;

CREATE TABLE Project_progress (
	Project_id SERIAL PRIMARY KEY,/* Project_id −− Unique key for sources in case we visit the same source more than once in the future.*/
	source_id VARCHAR(20) NOT NULL REFERENCES water_source(source_id) ON DELETE CASCADE ON UPDATE CASCADE, /* source_id −− Each of the sources we want to improve should exist, and should refer to the source table. This ensures data integrity.*/
	Address VARCHAR(50), -- Street address
	Town VARCHAR(30),
	Province VARCHAR(30),
	Source_type VARCHAR(50),
	Improvement VARCHAR(50), -- What the engineers should do at that place
	Source_status VARCHAR(50) DEFAULT 'Backlog' CHECK (Source_status IN ('Backlog', 'In progress', 'Complete')),
 -- Source_status −− We want to limit the type of information engineers can give us, so we limit Source_status.
	Date_of_completion DATE, -- Engineers will add this the day the source has been upgraded
	Comments TEXT -- Engineers can leave comments. We use a TEXT type that has no limit on char length
);
    
-- Create a view to query the Project_progress 
CREATE VIEW Inner_project_progress AS 
(SELECT
	water_source.source_id, 
    location.address,
	location.town_name,
	location.province_name,
	water_source.type_of_water_source,
    CASE 
		WHEN results = 'Contaminated: Biological' THEN "Install UV filter and RO filter" 
		WHEN results = 'Contaminated: Chemical' THEN "Install RO filter"
		WHEN type_of_water_source = 'river' THEN "Drill well"
        WHEN type_of_water_source = 'shared_tap' AND time_in_queue >= 30 THEN CONCAT("Install ", FLOOR(time_in_queue/30), " taps nearby")
        WHEN type_of_water_source = 'tap_in_home_broken' THEN "Diagnose local infrastructure"
    ELSE NULL END AS Improvement,
    well_pollution.results
FROM
	water_source
LEFT JOIN
	well_pollution ON water_source.source_id = well_pollution.source_id
INNER JOIN
	visits ON water_source.source_id = visits.source_id
INNER JOIN
	location ON location.location_id = visits.location_id
WHERE
	visits.visit_count = 1 -- This must always be true
	AND ( -- AND one of the following (OR) options must be true as well.
	results != 'Clean'
	OR type_of_water_source IN ('tap_in_home_broken','river')
	OR (type_of_water_source = 'shared_tap' AND visits.time_in_queue >= 30)
	)
);

-- Inserting the query results into Project_progress table
INSERT INTO project_progress(source_id, Address, Town, Province, Source_type, Improvement)
SELECT
	source_id, 
    address,
	town_name,
	province_name,
	type_of_water_source,
	Improvement
FROM Inner_project_progress;

-- Table to be used to update on project progress
SELECT * 
FROM project_progress;

