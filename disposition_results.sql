DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.allDispositions;
DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.statusResults;
DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.allDistributed;
DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.aggAllDistributed;
DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.aggDispositions;
DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.results;
DROP TEMPORARY TABLE
IF
	EXISTS data_distribution.aggResults;

	-- ************************** Concat dipso tables. !!!! ADD ALL !!!! **********************************
CREATE TEMPORARY TABLE data_distribution.allDispositions ( SELECT allDispos.data_source, allDispos.`status` FROM dispositions.dispositions AS allDispos );

-- SELECT * FROM data_distribution.allDispositions;
-- ******************************* JOIN status and rpc results ******************************************************
CREATE TEMPORARY TABLE data_distribution.statusResults (
	SELECT
		sm.status_code,
		sm.aps_status_id,
		ap.rpc 
	FROM
		dispositions.status_mapping AS sm
		LEFT JOIN dispositions.aps_status AS ap ON sm.aps_status_id = ap.aps_status_id 
	);
-- select * from data_distribution.statusResults;
-- ******************************************************************************************************************
ALTER TABLE data_distribution.allDispositions ADD COLUMN rpc INT ( 16 );
UPDATE data_distribution.allDispositions dispo
LEFT JOIN data_distribution.statusResults AS sr ON sr.status_code = dispo.`status` 
SET dispo.rpc = sr.rpc 
WHERE
	dispo.rpc IS NULL;

-- SELECT DISTINCT(status) FROM data_distribution.allDispositions WHERE rpc is null;
-- *********************************************** AGGREGATE disposition table *****************************************
CREATE TEMPORARY TABLE data_distribution.aggDispositions (
	SELECT
		alldispo.data_source,
		count( alldispo.data_source ) AS 'attempts',
		sum( rpc ) AS 'rpc' 
	FROM
		data_distribution.allDispositions AS alldispo 
	GROUP BY
		alldispo.data_source 
	);
	-- SELECT * FROM data_distribution.aggDispositions;
ALTER TABLE data_distribution.aggDispositions ADD PRIMARY KEY (data_source ( 128 ));

--  ******************************************************************************************************************
CREATE TEMPORARY TABLE data_distribution.allDistributed (
	SELECT
		dist.id_number,
		dist.data_source,
		dist.distributed_to,
		dist.expiry_date,
		dist.date_distributed,
		f.crm_name,
		ds.classification,
		app.applicationdate,
		nettapp.DateToQaOnly 
	FROM
		data_distribution.blc_distribution AS dist
		LEFT JOIN data_distribution.feeder AS f ON f.feeder_reference = dist.distributed_to
		LEFT JOIN data_distribution.data_source AS ds ON ds.data_source_id = dist.source_id
		LEFT JOIN blue_label_reporting.clientinfo AS app ON app.idnumber = dist.id_number 
		AND app.applicationdate >= dist.date_distributed 
		AND app.applicationdate <= dist.expiry_date 
		AND app.callcentre = f.crm_name
		LEFT JOIN blue_label_reporting.r_acquistions AS nettapp ON app.dapsagreementnumber = nettapp.AgreementNo 
	WHERE
		dist.distributed_to IN ( 117, 127, 133, 136 ) 
		AND date_distributed >= now()- INTERVAL 4 MONTH 
		AND data_source IS NOT NULL 
	AND dist.source_id NOT IN ( 'BS', 'PS', 'SS', 'SM', 'SR' ));
	
-- ***********************************************************************************************************
-- SELECT * FROM data_distribution.allDistributed;
SELECT
	data_source,
	COUNT(*) AS cnt 
FROM
	data_distribution.allDistributedalter TABLE data_distribution.aggAllDistributed ADD PRIMARY KEY (
	dsource ( 128 ));

GROUP BY
	data_source 
HAVING
	cnt > 1;*/ 

-- ************************************* AGGREGATE DISTRIBUTED *************************************************
CREATE TEMPORARY TABLE data_distribution.aggAllDistributed (
	SELECT
		id_number,
		data_source AS 'dsource',
		crm_name,
		date_distributed,
		classification,
		COUNT( applicationdate ) AS 'appCreated',
		COUNT( DateToQaOnly ) AS 'nettApp' 
	FROM
		data_distribution.allDistributed 
	GROUP BY
		data_source 
	);
ALTER TABLE data_distribution.aggAllDistributed ADD PRIMARY KEY (dsource ( 128 ));

-- ************************************ JOIN Disposition Onto Distributed ***************************************

-- SELECT * FROM data_distribution.aggAllDistributed WHERE crm_name = 'BFU Sales';
-- SELECT * FROM data_distribution.aggDispositions;

CREATE TEMPORARY TABLE data_distribution.results ( SELECT * FROM data_distribution.aggAllDistributed AS dist LEFT JOIN data_distribution.aggDispositions AS dispo ON dispo.data_source = dist.dsource );
-- ******************************************************* UPDATE *********************************
UPDATE data_distribution.results 
SET rpc =
CASE
		
		WHEN rpc > 1 THEN
		1 ELSE rpc 
	END 
	WHERE
		rpc > 1;
	UPDATE data_distribution.results 
	SET data_source =
	CASE
			
			WHEN data_source IS NOT NULL THEN
			1 ELSE 0 
		END;
        
-- ************************************* AGGREGATE RESULTS *******************************************************
	CREATE TEMPORARY TABLE data_distribution.aggResults (
		SELECT
			COUNT( id_number ) AS 'distributed',
			crm_name,
			date_distributed,
			classification,
			SUM( data_source ) AS 'uniqueDialed',
			SUM( rpc ) AS 'rpc',
			SUM( appCreated ) AS 'apps',
			SUM( nettApp ) 'nettApps',
			SUM( attempts ) AS 'totalAttempts' 
		FROM
			data_distribution.results 
		GROUP BY
			date_distributed,
			crm_name,
		classification 
	);
