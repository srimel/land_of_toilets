SELECT * FROM toilets LIMIT 20;

--1. How many toilets in Cooma have parking?
SELECT COUNT(*) AS "# Toilets in Cooma with Parking"
FROM location_rel JOIN locations USING(locID) JOIN town_rel USING(LocID)
JOIN towns USING(TownID) JOIN access USING(facilityID)
WHERE Town='Cooma' AND parking=True;

--2. How many public toilets are there in Australia?
SELECT COUNT(*) AS "# Toilets Down Under" FROM toilets;

--3. What are all the sporting facility toilets that are also dump points? // REWORD: How many sporting facility toilets are also dump points?
SELECT COUNT(*) AS "# Sporting Facilities That Double As Dumps"
FROM facility_rel JOIN facility_types FT USING(typeid)
JOIN dump_points USING (facilityID)
WHERE FT.name='Sporting facility';

--4. Which city has the most public toilets?
SELECT town, COUNT(facilityID) AS "Toilet Count" FROM
location_rel JOIN locations USING(locID) JOIN town_rel USING(locID) 
JOIN towns T USING(townID) 
GROUP BY town ORDER BY town;

--5. What percentage of toilets are open 24 hours a day?
SELECT 100*(SELECT COUNT(*) FROM toilets JOIN access USING(facilityID) WHERE openinghours='OPEN: 24 hours')/COUNT(*) 
AS "% Austrailian Toilets Open 24 Hours a Day" FROM toilets;

--6. How many toilets in WA require payment?
SELECT COUNT(*) AS "# Toilets in WA That Require Money" FROM access JOIN location_rel USING(facilityID) JOIN locations USING(locID)
JOIN state_rel USING(locID) JOIN states USING(stateID)
WHERE state='WA' AND paymentrequired=TRUE;

--7. How many unisex toilets are in a park or reserve?
SELECT count(*) AS "# Unisex Toilets in Parks and Reserves" FROM toilets 
JOIN facility_rel USING(FacilityID) JOIN facility_types T USING(typeID)
WHERE T.name='Park or reserve' AND unisex=True;


--8. How many toilets are on 1 Bay Street in Glebe?
SELECT COUNT(*) AS "# Toilets on 1 Bay Street in Glebe" FROM
location_rel JOIN locations USING(locID) JOIN town_rel USING(locID) JOIN towns USING(townid)
WHERE town='Glebe' AND address1='1 Bay Street';


--9. What percentage of public toilets are free?
SELECT 100*(SELECT COUNT(*) FROM access WHERE paymentrequired=False)/COUNT(*) AS  "Percentage of the Free" FROM toilets;


--10. What percentage of public toilets with baby changing stations are free?
SELECT 100*(SELECT COUNT(*) FROM access JOIN changing USING(facilityID) WHERE paymentrequired=False and babychange=True)/COUNT(*) AS  "Percentage of the Free... With Babies" 
FROM toilets JOIN changing USING(facilityID) WHERE babychange=True;

--11. What percentage of men’s toilets contain baby changing stations?
SELECT 100*(SELECT COUNT(*) FROM toilets JOIN changing USING(facilityID) WHERE male=True AND babychange=True)/COUNT(*) AS "% Men's Restrooms with Baby Changing Stations"
FROM toilets JOIN changing USING (facilityID) WHERE male=True AND babychange=False;

--11.5 What percentage of women's toilets contain baby changing stations?
SELECT 100*(SELECT COUNT(*) FROM toilets JOIN changing USING(facilityID) WHERE female=True AND babychange=True)/COUNT(*) AS "% Women's Restrooms with Baby Changing Stations"
FROM toilets JOIN changing USING (facilityID) WHERE female=True AND babychange=False;

--11.75 What percentage of unisex toilets contain baby changing stations?
SELECT 100*(SELECT COUNT(*) FROM toilets JOIN changing USING(facilityID) WHERE unisex=True AND babychange=True)/COUNT(*) AS "% Unisex Restrooms with Baby Changing Stations"
FROM toilets JOIN changing USING (facilityID) WHERE unisex=True AND babychange=False;


--12. How many carpark dump points are there?
--13. What is the average number of toilets per city?
--14. Are public toilets with showers more likely to have a fee than those without?

--15. How many toilets with sharp disposals are in every state?
SELECT state, COUNT(*) as "Toilets with sharp disposal"
FROM toilets natural join disposal natural join state_rel join states using(stateid)
WHERE  sharpsdisposal = true
GROUP BY(state);


--16. How many toilets that are accessible have parking?
SELECT COUNT(*) as "Number of accessible toilets with parking"
FROM toilets natural join access
WHERE accessible = true AND parking = true;

--17. Which toilets in VIC are ambulant?
SELECT facilityid, name, state
FROM toilets natural join location_rel natural join state_rel natural join states natural join handicap
WHERE ambulant = true AND state = 'VIC';

--18. How many restrooms have an accessible toilet?
SELECT COUNT(*) as "Numbe of toilets that are accessible"
FROM toilets natural join access
WHERE accessible = true;

--19. How many toilets have drinking water and showers?
SELECT COUNT(*) as "Number of toilets with water fountain and showers"
FROM toilets 
WHERE drinkingwater = true AND shower = true;

--20. How many toilets require the master locksmith’s access key (MLAK) to enter?
 SELECT COUNT(mlak24) as "Toilets requiring master locksmith's access key"
 FROM toilets join access using(facilityid)
 WHERE mlak24 = false;

