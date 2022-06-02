SELECT * FROM toilets LIMIT 20;
--1. How many toilets in Cooma have parking?
SELECT COUNT(*) FROM toilets
JOIN location_rel USING(FacilityID) JOIN locations USING(LocID) JOIN town_rel USING(LocID)
JOIN towns USING(TownID) JOIN access USING(facilityID)
WHERE Town='Cooma' AND parking=True;

--2. How many public toilets are there in Australia?
SELECT COUNT(*) FROM toilets;

--3. What are all the sporting facility toilets that are also dump points? // REWORD: How many sporting facility toilets are also dump points?
SELECT COUNT(*) FROM toilets 
JOIN facility_rel USING(facilityID) JOIN facility_types FT USING(typeid) 
JOIN dump_points USING (facilityID)
WHERE FT.name='Sporting facility';

--4. Which city has the most public toilets?
SELECT town, COUNT(facilityID) AS number_o_bathrooms FROM
location_rel JOIN locations USING(locID) JOIN town_rel USING(locID) 
JOIN towns T USING(townID) 
GROUP BY town ORDER BY town;


--5. What percentage of toilets are open 24 hours a day?
--6. How many toilets in WA require payment?
--7. How many unisex toilets are in a park or reserve?
--8. How many toilets are on 1 Bay Street in Glebe?
--9. What percentage of public toilets are free?
--10. What percentage of public toilets with baby changing stations are free?
--11. What percentage of men’s toilets contain baby changing stations?
--12. How many carpark dump points are there?
--13. What is the average number of toilets per city?
--14. Are public toilets with showers more likely to have a fee than those without?
--15. Which state has the most toilets with sharps disposal?
--16. Do most “park or reserve” toilets have parking?
--17. What percentage of toilets in VIC are ambulant?
--18. What percentage of toilets have an accessible toilet?
--19. Do most toilets with drinking water also have a shower?
--20. How many toilets require the master locksmith’s access key (MLAK) to enter?