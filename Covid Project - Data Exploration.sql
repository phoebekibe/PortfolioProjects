/*
Covid 19 Data Exploration 

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/

SELECT * 
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
ORDER BY 3,4;

/* SELECT *
FROM covidproject.covidvaccinations
ORDER BY 3,4; */

-- Select the data we are going to use
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2;

-- Looking at Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in a specific location
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM covidproject.coviddeaths
WHERE location LIKE "%states%" AND continent IS NOT NULL
ORDER BY 1,2;

-- Looking at Total Cases vs Population
-- Shows percentage of population infected with covid
SELECT location, date, population, total_cases, (total_cases/population)*100 AS PercentPopulationInfected
FROM covidproject.coviddeaths
WHERE location LIKE "%states%" AND continent IS NOT NULL
ORDER BY 1,2;

-- Countries with highest infection rate compared to population
SELECT location, population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 AS PercentPopulationInfected
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY PercentPopulationInfected DESC;

-- Countries with highest death count per population
ALTER TABLE covidproject.coviddeaths MODIFY COLUMN total_deaths INT;
SELECT location, MAX(total_deaths) as TotalDeathCount
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC;



-- BREAKING THINGS DOWN BY CONTINENT
-- Showing continents with highest death count per population
SELECT continent, MAX(total_deaths) as TotalDeathCount
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC;

-- GLOBAL NUMBERS BY DATE
ALTER TABLE covidproject.coviddeaths MODIFY COLUMN new_deaths INT;
SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS DeathPercentage
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2;

-- GLOBAL NUMBERS
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS DeathPercentage
FROM covidproject.coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2;

-- JOIN coviddeaths and covidvaccinations table
SELECT * 
FROM coviddeaths AS dea
JOIN covidvaccinations AS vac
	ON dea.location = vac.location AND dea.date = vac.date;
    
-- Total population vs Vaccinations
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY dea.location ORDER BY dea.date) AS RollingVaccinatedPeople
FROM coviddeaths AS dea
JOIN covidvaccinations AS vac
	ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3;


-- Showing percentage of population vaccinated
-- USE CTE (Method 1)
WITH 
	Popvsvac(continent, location, date, population, new_vaccinations, RollingVaccinatedPeople ) AS (
		SELECT dea.continent, dea.location,dea.date, dea.population, vac.new_vaccinations,
			SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY dea.location ORDER BY dea.date) AS RollingVaccinatedPeople
		FROM coviddeaths AS dea
		JOIN covidvaccinations AS vac
			ON dea.location = vac.location AND dea.date = vac.date
		WHERE dea.continent IS NOT NULL
		ORDER BY 2,3
    )
SELECT *, (RollingVaccinatedPeople/ population)*100 AS PercentVaccinated
FROM Popvsvac;



-- USE TEMP TABLE (Method 2)
DROP TABLE if exists PercentPopVaccinated;
CREATE TABLE PercentPopVaccinated
( continent varchar(255), location varchar(255), date datetime, population int, new_vaccinations int, RollingVaccinatedPeople float);

INSERT INTO PercentPopVaccinated
		SELECT dea.continent, dea.location,dea.date, dea.population, vac.new_vaccinations,
			SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY dea.location ORDER BY dea.date) AS RollingVaccinatedPeople
		FROM coviddeaths AS dea
		JOIN covidvaccinations AS vac
			ON dea.location = vac.location AND dea.date = vac.date
		--  WHERE dea.continent IS NOT NULL
		ORDER BY 2,3;
SELECT *, (RollingVaccinatedPeople/ population)*100 AS PercentVaccinated
FROM PercentPopVaccinated;



-- CREATING VIEWS TO STORE DATA FOR LATER VISUALIZATIONS
CREATE VIEW PopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY dea.location ORDER BY dea.date) AS RollingVaccinatedPeople
FROM coviddeaths AS dea
JOIN covidvaccinations AS vac
	ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3;

SELECT * 
FROM covidproject.populationvaccinated;





