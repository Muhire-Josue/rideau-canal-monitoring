WITH Aggregates AS (
    SELECT
        location,
        MIN(timestamp) AS windowStart,
        System.Timestamp AS windowEnd,
        AVG(iceThicknessCm)       AS avgIceThicknessCm,
        MIN(iceThicknessCm)       AS minIceThicknessCm,
        MAX(iceThicknessCm)       AS maxIceThicknessCm,
        AVG(surfaceTemperatureC)  AS avgSurfaceTemperatureC,
        MIN(surfaceTemperatureC)  AS minSurfaceTemperatureC,
        MAX(surfaceTemperatureC)  AS maxSurfaceTemperatureC,
        MAX(snowAccumulationCm)   AS maxSnowAccumulationCm,
        AVG(externalTemperatureC) AS avgExternalTemperatureC,
        COUNT(*)                  AS readingCount
    FROM
        iotInput TIMESTAMP BY timestamp
    GROUP BY
        TUMBLINGWINDOW(minute, 5),
        location
),
WithSafety AS (
    SELECT
        -- Use CAST instead of FORMATDATETIME to avoid the error
        CONCAT(
          REPLACE(location, ' ', '-'),
          '-',
          CAST(windowEnd AS nvarchar(max))
        ) AS id,
        location,
        windowStart,
        windowEnd,
        avgIceThicknessCm,
        minIceThicknessCm,
        maxIceThicknessCm,
        avgSurfaceTemperatureC,
        minSurfaceTemperatureC,
        maxSurfaceTemperatureC,
        maxSnowAccumulationCm,
        avgExternalTemperatureC,
        readingCount,
        CASE
            WHEN avgIceThicknessCm >= 30 AND avgSurfaceTemperatureC <= -2 THEN 'Safe'
            WHEN avgIceThicknessCm >= 25 AND avgSurfaceTemperatureC <= 0 THEN 'Caution'
            ELSE 'Unsafe'
        END AS safetyStatus
    FROM Aggregates
)

-- Cosmos DB output
SELECT *
INTO cosmosOutput
FROM WithSafety;

-- Blob Storage output
SELECT *
INTO blobOutput
FROM WithSafety;