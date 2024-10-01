/*
COVID-19 Data Exploration

Skills used: Joins, CTEs, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/

-- covid_deaths table:

SELECT * FROM covid_deaths;

-- Select data that we are going to be using:
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid_deaths
ORDER BY location, date;

-- Total Cases vs Total Deaths:

-- When total_cases is 0, the NULLIF() function returns 'NULL', so no division is attempted
-- Casted total_deaths as a float so calculation will display correctly

SELECT 
    location, 
    date, 
    total_cases, 
    total_deaths, 
    (total_deaths::float / NULLIF(total_cases, 0)) * 100 AS death_percentage
FROM 
    covid_deaths
ORDER BY 
    location, date;

-- Shows likelihood of dying (death_percentage) if you contract COVID-19 in the United States:

SELECT 
    location, 
    date, 
    total_cases, 
    total_deaths, 
    (total_deaths::float / NULLIF(total_cases, 0)) * 100 AS death_percentage
FROM 
    covid_deaths
WHERE location = 'United States'
ORDER BY 
    location, date;

-- Total Cases vs. Population in the United States:
-- show what percetage of population contracted COVID-19

SELECT 
    location,
	date,
    population,
	total_cases, 
    (total_cases::float / population::int) * 100 AS percent_population_infected
FROM 
    covid_deaths
WHERE location = 'United States'
ORDER BY 
    location, date;

-- Countries with Highest Infection rate compared to population:

SELECT 
    location,  
    population,
	MAX(total_cases) AS Highest_Infection_Count,
    MAX((total_cases::float / population::float)) * 100 AS Percent_Population_Infected
FROM 
    covid_deaths
GROUP BY
	location, population
ORDER BY 
    Percent_Population_Infected DESC;

-- Countries with the highest death count per population:

SELECT 
    location,  
	MAX(total_deaths::int) AS TotalDeathCount
FROM 
    covid_deaths
WHERE
	continent IS NOT NULL
GROUP BY
	location
ORDER BY 
    TotalDeathCount DESC;

	-- LET'S BREAK THINGS DOWN BY CONTINENT AND SPECIAL GROUPS (i.e., World total, high-income countries, low-income, etc.):
	
	SELECT 
	    location,  
		MAX(total_deaths::int) AS TotalDeathCount
	FROM 
	    covid_deaths
	WHERE
		continent IS NULL
	GROUP BY
		location
	ORDER BY 
	    TotalDeathCount DESC;
	
	-- LET'S BREAK THINGS DOWN ONLY BY CONTINENTS:
	
	SELECT 
	    continent,  
		MAX(total_deaths::int) AS TotalDeathCount
	FROM 
	    covid_deaths
	WHERE
		continent IS NOT NULL
	GROUP BY
		continent
	ORDER BY 
	    TotalDeathCount DESC;

-- GLOBAL NUMBERS:
-- Populates many nulls...

SELECT 
    date, 
    SUM(new_cases::int) AS total_cases,
	SUM(new_deaths::float) AS total_deaths,
	SUM(new_deaths::float)/NULLIF(SUM(new_cases::int), 0) * 100 AS death_percentage
FROM 
    covid_deaths
WHERE continent IS NOT NULL
GROUP BY
	date
ORDER BY 
    date; 

-- UPDATED QUERY WITH THE HELP FROM CHATGPT:
	-- CASE for death_percentage: If the sum of new_cases is zero, the division is bypassed, and we explicitly return 0 for the death percentage. This avoids unnecessary NULL results.
	-- HAVING clause: This filters out rows where both total_cases and total_deaths are zero, ensuring you only see dates with actual data.
	-- Handling NULL values: PostgreSQL's SUM() automatically ignores NULL values, but if you have rows with NULL data, ensure that those values represent "no data" or missing information.
	--This should fix the issue by removing dates where no cases or deaths were recorded, while providing accurate global sums and death percentages for the dates where data was reported.

SELECT 
    date, 
    SUM(new_cases::int) AS total_cases,
    SUM(new_deaths::float) AS total_deaths,
    CASE
        WHEN SUM(new_cases::int) = 0 THEN 0
        ELSE (SUM(new_deaths::float) / NULLIF(SUM(new_cases::int), 0)) * 100
    END AS death_percentage
FROM 
    covid_deaths
WHERE 
    continent IS NOT NULL
GROUP BY 
    date
HAVING 
    SUM(new_cases::int) > 0 OR SUM(new_deaths::float) > 0
ORDER BY 
    date;

-- Total population vs. Vaccinations:
-- Shows how many people have received at least one COVID-19 vaccine

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations::int) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS vaccination_count
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY dea.location, dea.date;


-- Using CTE to perform calculations on PARTITION BY in previous query
-- Show percentage of population that has received at least one COVID-19 vaccine

WITH PopvsVac (continent, location, date, population, new_vaccinations, vaccination_count)
AS
(
	SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations::int) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS vaccination_count
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY dea.location, dea.date
	)
SELECT *, 
	(vaccination_count / population::float)*100 AS vaccination_percentage
FROM PopvsVac;

-- Using Temp Table to perform calculation on PARTITION BY in previous query

	-- Drop the table if it exists
	DROP TABLE IF EXISTS percent_pop_vaccinated;
	
	-- Create the new table
	CREATE TABLE percent_pop_vaccinated
	(
	    continent VARCHAR(255),
	    location VARCHAR(255),
	    date DATE,
	    population BIGINT,
	    new_vaccinations BIGINT,
	    vaccination_count BIGINT
	);
	
	-- Insert data into the temp table
	INSERT INTO percent_pop_vaccinated
	SELECT 
	    dea.continent, 
	    dea.location, 
	    dea.date::date, 
	    dea.population::bigint, 
	    vac.new_vaccinations::bigint, 
	    SUM(vac.new_vaccinations::int) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS vaccination_count
	FROM 
	    covid_deaths AS dea
	JOIN 
	    covid_vaccinations AS vac
	    ON dea.location = vac.location
	    AND dea.date = vac.date
	WHERE 
	    dea.continent IS NOT NULL
	ORDER BY 
	    dea.location, dea.date;
	
	-- Select from the temp table and calculate the vaccination percentage
	SELECT *, 
	    (vaccination_count / population::float) * 100 AS vaccination_percentage
	FROM 
	    percent_pop_vaccinated;

-- Create View to store data for later visualizations:

CREATE VIEW Percent_Pop_Vaccinated AS
	SELECT 
	    dea.continent, 
	    dea.location, 
	    dea.date::date, 
	    dea.population::bigint, 
	    vac.new_vaccinations::bigint, 
	    SUM(vac.new_vaccinations::int) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS vaccination_count
	FROM 
	    covid_deaths AS dea
	JOIN 
	    covid_vaccinations AS vac
	    ON dea.location = vac.location
	    AND dea.date = vac.date
	WHERE 
	    dea.continent IS NOT NULL;
	-- ORDER BY 
	--     dea.location, dea.date;
