CREATE DATABASE advance_sql_course
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
;

C:\Program Files\PostgreSQL\14\bin\pg_restore.exe --host "localhost" --port "5432" --username "postgres" --no-password --dbname "advance_sql_course" --section=pre-data --section=data --section=post-data --verbose "C:\\Users\\KonuTech\\Desktop\\POSGRE~1\\ADVANC~1\\ADVANC~1.SQL"

SELECT *
FROM (
    SELECT *
    FROM general_hospital.patients
    WHERE date_of_birth >= '2000-01-01'
    ORDER BY master_patient_id
    ) AS p
WHERE p.name ilike 'm%'
;

SELECT se.*
FROM (
    SELECT *
    FROM general_hospital.surgical_encounters
    WHERE surgical_admission_date between '2016-11-01' and '2016-11-30'
    ) AS se
INNER JOIN (
    SELECT master_patient_id
    FROM general_hospital.patients
    WHERE date_of_birth >= '1990-01-01'
    ) AS p
ON se.master_patient_id = p.master_patient_id
;



WITH young_patients AS (
    SELECT *
    FROM general_hospital.patients
    WHERE date_of_birth >= '2000-01-01'
)
SELECT *
FROM young_patients
WHERE name ILIKE 'm%'
;


WITH
top_counties AS (
    SELECT
         county
        ,COUNT(*) AS num_patients
    FROM general_hospital.patients
    GROUP BY county
    HAVING COUNT(*) > 1500
),

county_patients AS (
    SELECT
         p.master_patient_id
        ,p.county
    FROM general_hospital.patients AS p
    INNER JOIN top_counties AS tc
    ON p.county = tc.county
)

SELECT
     p.county
    ,COUNT(se.surgery_id) AS num_surgeries
FROM general_hospital.surgical_encounters AS se
INNER JOIN county_patients AS p
ON se.master_patient_id = p.master_patient_id
GROUP BY
    p.county
;


WITH
total_cost AS (
    SELECT
         surgery_id
        ,SUM(resource_cost) AS total_surgery_cost
    FROM general_hospital.surgical_costs
    GROUP BY surgery_id
)

SELECT *
FROM total_cost
WHERE total_surgery_cost > (
    SELECT
         AVG(total_surgery_cost)
    FROM total_cost
)
;



    SELECT *
    FROM general_hospital.vitals
    WHERE
        bp_diastolic >
            (
                SELECT
                     MIN(bp_diastolic) FROM general_hospital.vitals
            )
    AND
        bp_systolic >
            (
                SELECT
                     MAX(bp_diastolic) FROM general_hospital.vitals
            )
;


    SELECT *
    FROM general_hospital.patients
    WHERE
        master_patient_id IN (
            SELECT
                DISTINCT master_patient_id
            FROM general_hospital.surgical_encounters
        )
    ORDER BY
        master_patient_id
;



    SELECT *
    FROM general_hospital.patients AS p
    INNER JOIN general_hospital.surgical_encounters AS se
    ON p.master_patient_id = se.master_patient_id
    ORDER BY
        p.master_patient_id
;



SELECT *
FROM general_hospital.surgical_encounters
WHERE total_profit > ALL (
    SELECT
        AVG(total_cost)
    FROM general_hospital.surgical_encounters
    GROUP BY discharge_description
)
;

SELECT
     diagnosis_description
    ,AVG(surgical_discharge_date - surgical_admission_date) AS length_of_stay                -- date
FROM general_hospital.surgical_encounters
GROUP BY diagnosis_description
HAVING AVG(surgical_discharge_date - surgical_admission_date) <= 
    ALL(
        SELECT
             AVG(EXTRACT(DAY FROM patient_discharge_datetime - patient_admission_datetime))  -- timestamp
        FROM general_hospital.encounters
        GROUP BY department_id
    )
;


SELECT
     unit_name
    ,string_agg(DISTINCT surgical_type, ',') AS case_types
FROM general_hospital.surgical_encounters
GROUP BY unit_name
HAVING string_agg(DISTINCT surgical_type, ',') LIKE ALL (
    SELECT string_agg(DISTINCT surgical_type, ',')
    FROM general_hospital.surgical_encounters
)
;

-- select distinct  surgical_type from general_hospital.surgical_encounters;


SELECT
    e.*
FROM general_hospital.encounters AS e
WHERE EXISTS (
    SELECT
        1
    FROM general_hospital.orders_procedures AS o
    WHERE e.patient_encounter_id = o.patient_encounter_id
)
;


SELECT
    p.*
FROM general_hospital.patients AS p
WHERE NOT EXISTS (
    SELECT
        1
    FROM general_hospital.surgical_encounters AS se
    WHERE se.master_patient_id = p.master_patient_id
)
;

-- RECURSIVE CTE

WITH RECURSIVE fibonacci AS (
    SELECT
         1 AS a
        ,1 AS b
    UNION ALL
    SELECT
         b
        ,a + b
    FROM fibonacci
)
SELECT
     a
    ,b
FROM fibonacci
limit 15
;


WITH RECURSIVE orders AS (
    SELECT
         order_procedure_id
        ,order_parent_order_id
        ,0 AS level
    FROM general_hospital.orders_procedures
    WHERE order_parent_order_id IS NULL
    UNION ALL
    SELECT
         op.order_procedure_id
        ,op.order_parent_order_id
        ,o.level + 1 AS level
    FROM general_hospital.orders_procedures AS op
    INNER JOIN orders AS o ON op.order_parent_order_id = o.order_procedure_id
)
SELECT
     *
FROM orders
WHERE level <> 0
;



SELECT
     orders_count.patient_encounter_id
    ,AVG(orders_count.number_of_orders) AS average_number_of_orders
FROM
    (
    SELECT
    --      e.master_patient_id
        e.patient_encounter_id
        ,op.ordering_provider_id
        ,COUNT(op.order_cd) AS number_of_orders
    FROM general_hospital.encounters AS e
    LEFT JOIN general_hospital.orders_procedures AS op
    ON e.patient_encounter_id = op.patient_encounter_id
    GROUP BY
        e.patient_encounter_id
        ,op.ordering_provider_id
    ) AS orders_count
GROUP BY
     orders_count.patient_encounter_id
ORDER BY
    orders_count.patient_encounter_id
;



WITH
    provider_encounters AS (
    SELECT
         patient_encounter_id
        ,ordering_provider_id
        ,COUNT(order_procedure_id) AS num_procedures
    FROM general_hospital.orders_procedures
    GROUP BY
         ordering_provider_id
        ,patient_encounter_id
    ),

    provider_orders AS (
    SELECT
         ordering_provider_id
        ,AVG(num_procedures) AS avg_num_procedures
    FROM provider_encounters
    GROUP BY
        ordering_provider_id
    )

SELECT
     p.full_name
    ,po.avg_num_procedures
FROM general_hospital.physicians AS p
LEFT OUTER JOIN provider_orders AS po
ON p.id = po.ordering_provider_id
WHERE po.avg_num_procedures IS NOT NULL
ORDER BY po.avg_num_procedures DESC
;


SELECT DISTINCT
    patient_encounter_id
FROM general_hospital.orders_procedures
WHERE order_cd IN (
    SELECT
        order_cd
    FROM general_hospital.orders_procedures
    GROUP BY order_cd
    ORDER BY COUNT(*) DESC
    LIMIT 10
);



SELECT DISTINCT
     a.account_id
    ,a.total_account_balance
FROM general_hospital.accounts AS a
WHERE total_account_balance > 10000
AND EXISTS (
    SELECT 1
    FROM general_hospital.encounters AS e
    WHERE e.hospital_account_id = a.account_id
    AND patient_in_icu_flag = 'Yes'
);


WITH
    old_los AS (
        SELECT
             EXTRACT(YEAR FROM AGE(NOW(), p.date_of_birth)) AS age
            ,AVG(se.surgical_discharge_date - se.surgical_admission_date) AS avg_los
        FROM general_hospital.patients AS p
        INNER JOIN general_hospital.surgical_encounters AS se
        ON p.master_patient_id = se.master_patient_id
        WHERE p.date_of_birth IS NOT NULL
        AND EXTRACT(YEAR FROM AGE(NOW(), p.date_of_birth)) >= 65
        GROUP BY EXTRACT(YEAR FROM AGE(NOW(), p.date_of_birth))
    )
SELECT
    e.*
FROM general_hospital.encounters AS e
INNER JOIN general_hospital.patients AS p
ON e.master_patient_id = p.master_patient_id AND p.date_of_birth >= '1995-01-01'
WHERE EXTRACT(days FROM(e.patient_discharge_datetime - e.patient_admission_datetime)) >= ALL(
    SELECT
        avg_los
    FROM old_los
)
;


WITH

surgical_los AS (
    SELECT
         surgery_id
        ,(surgical_discharge_date - surgical_admission_date) AS los
        ,AVG(surgical_discharge_date - surgical_admission_date) OVER() AS avg_los
    FROM general_hospital.surgical_encounters
)

SELECT
     *
    ,ROUND(los - avg_los, 2) AS over_under
FROM surgical_los
ORDER BY over_under DESC
;


SELECT
     account_id
    ,primary_icd
    ,total_account_balance
    ,RANK() OVER(PARTITION BY primary_icd ORDER BY total_account_balance DESC) AS account_rank_by_icd
FROM general_hospital.accounts
;


SELECT
     se.surgery_id
    ,p.full_name
    ,se.total_profit
    ,AVG(total_profit) OVER w AS avg_total_profit
    ,se.total_cost
    ,SUM(total_cost) OVER w AS total_surgeaon_cost
FROM general_hospital.surgical_encounters AS se
LEFT OUTER JOIN general_hospital.physicians AS p 
ON se.surgeon_id = p.id
WINDOW w AS (PARTITION BY se.surgeon_id)
ORDER BY total_surgeaon_cost DESC
; 


SELECT
     se.surgery_id
    ,p.full_name
    ,se.total_cost
    ,RANK() OVER (PARTITION BY surgeon_id ORDER BY total_cost ASC) AS cost_rank
    ,diagnosis_description
    ,total_profit
    ,ROW_NUMBER() OVER (PARTITION BY surgeon_id, diagnosis_description ORDER BY total_profit DESC) AS profit_row_number
FROM general_hospital.surgical_encounters AS se
LEFT OUTER JOIN general_hospital.physicians AS p 
ON se.surgeon_id = p.id
ORDER BY
     se.surgeon_id
    ,se.diagnosis_description
;



SELECT
     patient_encounter_id
    ,master_patient_id
    ,patient_admission_datetime
    ,LEAD(patient_admission_datetime) OVER w AS next_admission_date
    ,patient_discharge_datetime
    ,LAG(patient_discharge_datetime) OVER w AS previous_discharge_date
FROM general_hospital.encounters AS e
WINDOW W AS (PARTITION BY master_patient_id ORDER BY patient_admission_datetime)
ORDER BY
     master_patient_id
    ,patient_admission_datetime
;


WITH

surgeries_lagged AS (
    SELECT
         master_patient_id
        ,surgery_id
        ,surgical_admission_date
        ,surgical_discharge_date
        ,LAG(surgical_discharge_date) OVER (PARTITION BY master_patient_id ORDER BY surgical_admission_date) AS previous_discharge_date
    FROM general_hospital.surgical_encounters
)

SELECT
     *
    ,(surgical_admission_date - previous_discharge_date) AS days_between_surgeries
FROM surgeries_lagged
WHERE (surgical_admission_date - previous_discharge_date) <= 30
;



WITH

provider_department AS (
    SELECT
         admitting_provider_id
        ,department_id
        ,COUNT(*) AS num_encounters
    FROM general_hospital.encounters
    GROUP BY
         admitting_provider_id
        ,department_id
),

pd_ranked AS (
    SELECT
         *
        ,ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY num_encounters DESC) AS encounter_rank
    FROM provider_department
    
)

SELECT
     d.department_name
    ,p.full_name AS physician_name
    ,num_encounters
    ,encounter_rank
FROM pd_ranked AS pr
LEFT OUTER JOIN general_hospital.physicians AS p
ON p.id = pr.admitting_provider_id
LEFT OUTER JOIN general_hospital.departments AS d
ON d.department_id = pr.department_id
WHERE encounter_rank <= 3
;


WITH

total_cost AS (
    SELECT
         surgery_id
        ,resource_name
        ,resource_cost
        ,SUM(resource_cost) OVER (PARTITION BY surgery_id) AS total_surgery_cost_of_resource
    FROM general_hospital.surgical_costs
)

SELECT
     *
    ,ROUND(((resource_cost / total_surgery_cost_of_resource) * 100), 2) AS pct_total_cost_of_resource
FROM total_cost
WHERE (resource_cost / total_surgery_cost_of_resource) * 100 > 50
;



SELECT
     se1.surgery_id AS surgery_id1
    ,(se1.surgical_discharge_date - se1.surgical_admission_date) AS los1
    ,se2.surgery_id AS surgert_id2
    ,(se2.surgical_discharge_date - se2.surgical_admission_date) AS los2
FROM general_hospital.surgical_encounters AS se1
INNER JOIN general_hospital.surgical_encounters AS se2
ON (se1.surgical_discharge_date - se1.surgical_admission_date) = (se2.surgical_discharge_date - se2.surgical_admission_date)
WHERE (se1.surgical_discharge_date - se1.surgical_admission_date) >=30
;



SELECT
     o1.order_procedure_id
    ,o1.order_procedure_description
    ,o1.order_parent_order_id
    ,o2.order_procedure_description
FROM general_hospital.orders_procedures AS o1
INNER JOIN general_hospital.orders_procedures AS o2
ON o1.order_parent_order_id = o2.order_procedure_id
;


SELECT
     h.hospital_name
    ,d.department_name
FROM general_hospital.hospitals AS h
CROSS JOIN general_hospital.departments AS d
;


SELECT
     d.department_id
    ,d.department_name
FROM general_hospital.departments AS d
FULL JOIN general_hospital.hospitals AS h
ON d.hospital_id = h.hospital_id
WHERE
     h.hospital_id IS NULL
;


SELECT
     a.account_id
    ,e.patient_encounter_id
FROM general_hospital.accounts AS a
FULL JOIN general_hospital.encounters AS e
ON a.account_id = e.hospital_account_id
WHERE
      a.account_id IS NULL
     OR e.patient_encounter_id IS NULL
;


SELECT
     h.hospital_name
    ,d.department_name
FROM general_hospital.departments AS d
INNER JOIN general_hospital.hospitals AS h USING (hospital_id)
;


SELECT
     h.hospital_name
    ,d.department_name
FROM general_hospital.departments AS d
NATURAL JOIN general_hospital.hospitals AS h
;


SELECT
     p.full_name AS physician_name
    ,pr.name AS practice_name
FROM general_hospital.physicians AS p
CROSS JOIN general_hospital.practices AS pr
;


SELECT
     p.full_name AS physician_name
    ,AVG(v.bp_systolic) AS avg_systlic
    ,AVG(v.bp_diastolic) AS avg_diastolic
FROM general_hospital.vitals AS v
INNER JOIN general_hospital.encounters AS e USING (patient_encounter_id)
LEFT OUTER JOIN general_hospital.physicians AS p
ON e.admitting_provider_id = p.id
GROUP BY p.full_name
;

SELECT
    COUNT(DISTINCT sc.surgery_id)
FROM general_hospital.surgical_costs AS sc
FULL JOIN general_hospital.surgical_encounters AS se USING (surgery_id)
WHERE
    se.surgery_id IS NULL -- MISSING IDs
;


    SELECT
         surgery_id
    FROM general_hospital.surgical_encounters

UNION

    SELECT
        surgery_id
    FROM general_hospital.surgical_costs

ORDER BY surgery_id
;


    SELECT
        surgery_id
    FROM general_hospital.surgical_encounters

INTERSECT

    SELECT
        surgery_id
    FROM general_hospital.surgical_costs

ORDER BY
    surgery_id
;

WITH
    all_patients AS (
        SELECT
            master_patient_id
        FROM general_hospital.encounters
        
        INTERSECT
        
        SELECT
            master_patient_id
        FROM general_hospital.surgical_encounters
    )
SELECT
     ap.master_patient_id
    ,p.name
FROM all_patients AS ap
LEFT OUTER JOIN general_hospital.patients AS p
ON ap.master_patient_id = p.master_patient_id
;

    SELECT
         surgery_id
    FROM general_hospital.surgical_costs AS sc

EXCEPT

    SELECT
         surgery_id
    FROM general_hospital.surgical_encounters AS se

ORDER BY
    surgery_id
;



WITH
    missing_departments AS (
            SELECT
                department_id
            FROM general_hospital.departments
        
        EXCEPT
        
            SELECT
                department_id
            FROM general_hospital.encounters
    )
SELECT
     m.department_id
    ,d.department_name
FROM missing_departments AS m
LEFT OUTER JOIN general_hospital.departments AS d
ON m.department_id = d.department_id
;


WITH
    providers AS (
            SELECT
                 admitting_provider_id AS provider_id
                ,'Admitting' AS provider_type
            FROM general_hospital.encounters
        
        UNION
        
            SELECT
                 discharging_provider_id
                ,'Discharging' AS provider_type
            FROM general_hospital.encounters

        UNION
        
            SELECT
                 attending_provider_id
                ,'Attending' AS provider_type
            FROM general_hospital.encounters
    )
SELECT
      p.provider_id
     ,p.provider_type
     ,ph.full_name
FROM providers AS p
LEFT OUTER JOIN general_hospital.physicians AS ph
ON p.provider_id = ph.id
ORDER BY
     ph.full_name
    ,p.provider_type
;


WITH
    admitting_pcps AS (
            SELECT
                 pcp_id
            FROM general_hospital.patients
        
        INTERSECT
        
            SELECT
                admitting_provider_id
            FROM general_hospital.encounters
    )
    
SELECT
     a.pcp_id
    ,p.full_name
FROM admitting_pcps AS a
LEFT OUTER JOIN general_hospital.physicians AS p
ON a.pcp_id = p.id
;



            SELECT
                 surgeon_id
            FROM general_hospital.surgical_encounters
        
        EXCEPT
        
            SELECT
                id
            FROM general_hospital.physicians

;


SELECT
     state
    ,county
    ,COUNT(*) AS num_patients
FROM general_hospital.patients
GROUP BY GROUPING SETS (
     (state)
    ,(state, county)
    ,()
)
ORDER BY
     state DESC
    ,county
;


SELECT
     p.full_name
    ,se.admission_type
    ,se.diagnosis_description
    ,COUNT(*) AS num_surgeries
    ,AVG(total_profit) AS avg_total_profit
FROM general_hospital.surgical_encounters AS se
LEFT OUTER JOIN general_hospital.physicians AS p
    ON se.surgeon_id = p.id
GROUP BY GROUPING SETS (
     (p.full_name)
    ,(se.admission_type)
    ,(se.diagnosis_description)
    ,(p.full_name, se.admission_type)
    ,(p.full_name, se.diagnosis_description)
)
;


SELECT
     p.full_name
    ,se.admission_type
    ,se.diagnosis_description
    ,COUNT(*) AS num_surgeries
    ,AVG(total_profit) AS avg_total_profit
FROM general_hospital.surgical_encounters AS se
LEFT OUTER JOIN general_hospital.physicians AS p
    ON se.surgeon_id = p.id
GROUP BY CUBE (
     p.full_name
    ,se.admission_type
    ,se.diagnosis_description
)
;


SELECT
      h.state
     ,h.hospital_name
     ,d.department_name
     ,COUNT(e.patient_encounter_id) AS num_encounters
FROM general_hospital.encounters AS e
LEFT OUTER JOIN general_hospital.departments AS d
    ON e.department_id = d.department_id
LEFT OUTER JOIN general_hospital.hospitals AS h
    ON d.hospital_id = h.hospital_id
GROUP BY ROLLUP (
      h.state
     ,h.hospital_name
     ,d.department_name
)
ORDER BY
      h.state DESC
     ,h.hospital_name
     ,d.department_name
;

SELECT
      state
     ,county
     ,city
     ,COUNT(master_patient_id) AS num_patients
     ,AVG(EXTRACT(YEAR FROM age(now(), date_of_birth))) AS avg_age
FROM general_hospital.patients AS p
GROUP BY ROLLUP (
      state
     ,county
     ,city
)
ORDER BY
      state DESC
     ,county
     ,city
;


SELECT
      weight
     ,height
     ,AVG(pulse) AS avg_pulse
     ,AVG(body_surface_area) As avg_bsa
FROM general_hospital.vitals AS v
GROUP BY CUBE (
     weight
    ,height
)
ORDER BY
     height
    ,weight
;



SELECT
     DATE_PART('year', surgical_admission_date) AS year
    ,DATE_PART('month', surgical_admission_date) AS month
    ,DATE_PART('day', surgical_admission_date) AS day
    ,COUNT(surgery_id) AS num_surgeries
FROM general_hospital.surgical_encounters AS se
GROUP BY ROLLUP (
     1
    ,2
    ,3
)
ORDER BY
     1
    ,2
    ,3
;


SELECT
    *
FROM information_schema.tables
WHERE table_schema = 'general_hospital'
ORDER BY
    table_name
;


SELECT
     table_name
    ,data_type
    ,COUNT(*) AS num_columns
FROM information_schema.columns
WHERE
        table_schema = 'general_hospital'
GROUP BY
     table_name
    ,data_type
ORDER BY
     table_name
    ,3 desc
;

COMMENT ON TABLE general_hospital.vitals IS 'Patient vital data taken at the beginning of the encounter'
;

SELECT obj_description('general_hospital.vitals'::regclass)
;

COMMENT ON COLUMN general_hospital.accounts.primary_icd IS 'Primary International Classification of Diseases (ICD) code for the account'
;


SELECT *
FROM information_schema.columns
WHERE table_schema = 'general_hospital'
AND table_name = 'accounts'
;


SELECT col_description('general_hospital.accounts'::regclass, 1)
;


SELECT *
FROM information_schema.table_constraints
WHERE
    table_schema = 'general_hospital'
    AND table_name = 'surgical_encounters';
;


ALTER TABLE general_hospital.surgical_encounters
DROP CONSTRAINT check_positive_cost
;


SELECT *
FROM information_schema.table_constraints
WHERE table_schema = 'general_hospital'
    AND table_name = 'encounters'
    AND constraint_type = 'FOREIGN KEY'
ORDER BY
    constraint_name
;


ALTER TABLE general_hospital.encounters
DROP CONSTRAINT enocunters_attending_provider_id_fk
;


COMMENT ON COLUMN general_hospital.accounts.admit_icd is
'Admitting diagnosis code from the International Classification of Diseases'
;

ALTER TABLE general_hospital.surgical_encounters
ALTER COLUMN surgical_admission_date
SET NOT NULL
;

ALTER TABLE general_hospital.encounters
ADD CONSTRAINT check_discharge_after_admission
CHECK (
        (patient_admission_datetime < patient_discharge_datetime)
    OR
        (patient_discharge_datetime IS NULL)
)
;

ALTER TABLE general_hospital.surgical_encounters
ALTER COLUMN surgical_admission_date
DROP NOT NULl
;

ALTER TABLE general_hospital.encounters
DROP CONSTRAINT IF EXISTS check_discharge_after_admission
;


UPDATE general_hospital.vitals
	SET bp_diastolic = 100
	WHERE patient_encounter_id = 1854663
;


UPDATE general_hospital.accounts
	SET total_account_balance = 0
	WHERE account_id = 11417340
;


BEGIN TRANSACTION;
SELECT NOW();
END TRANSACTION;


BEGIN TRANSACTION;
UPDATE general_hospital.physicians
SET
     first_name = 'Bill'
    ,full_name = CONCAT(last_name, ', Bill')
WHERE ID = 1;
END TRANSACTION;
;




BEGIN TRANSACTION;
SELECT NOW();
UPDATE general_hospital.physicians
SET
     first_name = 'Gage'
    ,full_name = CONCAT(last_name, ', Gage')
WHERE ID = 1;
ROLLBACK;


BEGIN;
UPDATE general_hospital.vitals
    SET bp_diastolic = 120
    WHERE patient_encounter_id = 2570046;
SAVEPOINT vitals_updated;
UPDATE general_hospital.accounts
    SET total_account_balance = 1000
    WHERE account_id = 11417340;
ROLLBACK TO vitals_updated;
COMMIT;
SELECT
    *
FROM general_hospital.vitals
WHERE patient_encounter_id = '2570046'
;
SELECT
    *
FROM general_hospital.accounts
WHERE account_id = 11417340
;


BEGIN TRANSACTION;

UPDATE general_hospital.vitals
        SET bp_diastolic = 52
        WHERE patient_encounter_id = 1854663;

SAVEPOINT vitals_updated;

UPDATE general_hospital.accounts
        SET total_account_balance = 1000
        WHERE account_id = 11417340;

RELEASE SAVEPOINT vitals_updated;

COMMIT;
SELECT *
FROM general_hospital.accounts
WHERE account_id = 11417340;
;


BEGIN TRANSACTION;
SELECT NOW();
LOCK TABLE general_hospital.physicians;
END;
ROLLBACK;


BEGIN TRANSACTION;
LOCK TABLE general_hospital.physicians;
UPDATE general_hospital.physicians
    SET
         first_name = 'Gage'
        ,full_name = CONCAT(last_name, ', Gage')
    WHERE ID = 1;
COMMIT;

BEGIN TRANSACTION;
DROP TABLE general_hospital.practices;
ROLLBACK;


BEGIN TRANSACTION;
UPDATE general_hospital.accounts
    SET total_account_balance = 15077.90
    WHERE account_id = 11417340;
SAVEPOINT account_update;
DROP TABLE general_hospital.vitals;
ROLLBACK TO account_update
;


DROP TABLE IF EXISTS general_hospital.surgical_encounters_partitioned;
CREATE TABLE general_hospital.surgical_encounters_partitioned (
     surgery_id integer NOT NULL
    ,master_patient_id integer not null
    ,surgical_admission_date date not null
    ,surgical_discharge_discharge date
) PARTITION BY RANGE(surgical_admission_date)
;
CREATE TABLE surgical_encounters_y2016
    PARTITION OF general_hospital.surgical_encounters_partitioned
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE surgical_encounters_y2017
    PARTITION OF general_hospital.surgical_encounters_partitioned
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE surgical_encounters_default
    PARTITION OF general_hospital.surgical_encounters_partitioned
    default;
;
INSERT INTO general_hospital.surgical_encounters_partitioned
    SELECT
         surgery_id
        ,master_patient_id
        ,surgical_admission_date
        ,surgical_discharge_date
    FROM general_hospital.surgical_encounters
;
CREATE INDEX ON general_hospital.surgical_encounters_partitioned
    (surgical_admission_date)
;
SELECT
     EXTRACT(YEAR FROM surgical_admission_date)
    ,COUNT(*)
    FROM general_hospital.surgical_encounters
GROUP BY 1
;
SELECT
     COUNT(*)
    ,MIN(surgical_admission_date)
    ,MAX(surgical_admission_date)
    FROM surgical_encounters_y2016
;
SELECT
     COUNT(*)
    ,MIN(surgical_admission_date)
    ,MAX(surgical_admission_date)
    FROM surgical_encounters_y2017
;
SELECT
     COUNT(*)
    ,MIN(surgical_admission_date)
    ,MAX(surgical_admission_date)
    FROM surgical_encounters_default
;



CREATE TABLE general_hospital.departments_partitioned (
     hospital_id integer not null
    ,department_id integer not null
    ,department_name text
    ,specialty_description text
) PARTITION BY list (hospital_id)
;
SELECT DISTINCT hospital_id
FROM general_hospital.departments
;
CREATE TABLE departments_h111000
    PARTITION OF general_hospital.departments_partitioned
    FOR VALUES IN (111000);
CREATE TABLE departments_h112000
    PARTITION OF general_hospital.departments_partitioned
    FOR VALUES IN (112000);
CREATE TABLE departments_default
    PARTITION OF general_hospital.departments_partitioned
    DEFAULT;

INSERT INTO general_hospital.departments_partitioned
    SELECT
         hospital_id
        ,department_id
        ,department_name
        ,specialty_description
    FROM general_hospital.departments
;
SELECT
    hospital_id
    ,COUNT(*)
FROM departments_h111000
GROUP BY
    1
;
SELECT
    hospital_id
    ,COUNT(*)
FROM departments_h112000
GROUP BY
    1
;
SELECT
    hospital_id
    ,COUNT(*)
FROM departments_default
GROUP BY
    1
;



CREATE TABLE general_hospital.orders_procedures_partitioned (
     order_procedrure_id integer not null
    ,patient_encounter_id integer not null
    ,ordering_provider_id integer references general_hospital.physicians (id)
    ,order_cd text
    ,order_procedure_description text
) PARTITION BY hash (order_procedrure_id, patient_encounter_id)
;
CREATE TABLE general_hospital.orders_procedures_hash0
    PARTITION OF general_hospital.orders_procedures_partitioned
    FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE general_hospital.orders_procedures_hash1
    PARTITION OF general_hospital.orders_procedures_partitioned
    FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE general_hospital.orders_procedures_hash2
    PARTITION OF general_hospital.orders_procedures_partitioned
    FOR VALUES WITH (modulus 3, remainder 2);
INSERT INTO general_hospital.orders_procedures_partitioned
SELECT
     order_procedure_id
    ,patient_encounter_id
    ,ordering_provider_id
    ,order_cd
    ,order_procedure_description  
FROM general_hospital.orders_procedures
;
    SELECT
         'hash0'
         ,COUNT(*)
    FROM general_hospital.orders_procedures_hash0

UNION

    SELECT
         'hash1'
         ,COUNT(*)
    FROM general_hospital.orders_procedures_hash1

UNION

    SELECT
         'hash2'
         ,COUNT(*)
    FROM general_hospital.orders_procedures_hash2
;


CREATE TABLE IF NOT EXISTS general_hospital.visit (
     ID serial NOT NULL PRIMARY KEY
    ,start_dateitme timestamp
    ,end_datetime timestamp
);
CREATE TABLE IF NOT EXISTS general_hospital.emergency_visit (
     emergency_department_id int NOT NULL
    ,triage_level int
    ,triage_detatime timestamp
) inherits (visit)
;
INSERT INTO general_hospital.emergency_visit values
    (default, '2022-01-01 12:00:00', null, 12, 3, null)
;
INSERT INTO general_hospital.visit values
    (default, '2022-03-01 12:00:00', '2022-03-03 12:00:00')
;
SELECT *
FROM general_hospital.emergency_visit
;
SELECT *
FROM general_hospital.visit
;
INSERT INTO general_hospital.emergency_visit VALUES
    (2, '2022-03-01 11:00:00', '2022-03-03 12:00:00', 1, 1, null)
SELECT *
FROM ONLY general_hospital.emergency_visit
;


DROP TABLE IF EXISTS general_hospital.encounters_partitioned;
CREATE TABLE IF NOT EXISTS general_hospital.encounters_partitioned (
     hospital_id int NOT NULL
    ,patient_encounter_id int NOT NULL
    ,master_patient_id int NULL
    ,admitting_provider_id int REFERENCES general_hospital.physicians (id)
    ,department_id int REFERENCES general_hospital.departments (department_id)
    ,patient_admission_datetime timestamp
    ,patient_discharge_datetime timestamp
    ,CONSTRAINT encounters_partitioned_pk PRIMARY KEY
        (
             hospital_id
            ,patient_encounter_id
        )
    )
    PARTITION BY list (
        hospital_id
    )
;
SELECT DISTINCT
    d.hospital_id
FROM general_hospital.encounters AS e
LEFT OUTER JOIN general_hospital.departments AS d
ON e.department_id = d.department_id
ORDER BY 1
;
CREATE TABLE IF NOT EXISTS general_hospital.encounters_h111000
    PARTITION OF general_hospital.encounters_partitioned
    FOR VALUES IN (111000);
CREATE TABLE IF NOT EXISTS general_hospital.encounters_h112000
    PARTITION OF general_hospital.encounters_partitioned
    FOR VALUES IN (112000);
CREATE TABLE IF NOT EXISTS general_hospital.encounters_h114000
    PARTITION OF general_hospital.encounters_partitioned
    FOR VALUES IN (114000);
CREATE TABLE IF NOT EXISTS general_hospital.encounters_h115000
    PARTITION OF general_hospital.encounters_partitioned
    FOR VALUES IN (115000);
CREATE TABLE IF NOT EXISTS general_hospital.encounters_h9900006
    PARTITION OF general_hospital.encounters_partitioned
    FOR VALUES IN (9900006);
CREATE TABLE IF NOT EXISTS general_hospital.encounters_partitioned
    PARTITION OF general_hospital.encounters_partitioned
    DEFAULT;
INSERT INTO general_hospital.encounters_partitioned
SELECT
     d.hospital_id
    ,e.patient_encounter_id
    ,e.master_patient_id
    ,e.admitting_provider_id
    ,e.department_id
    ,e.patient_admission_datetime
    ,e.patient_discharge_datetime
FROM general_hospital.encounters AS e
LEFT OUTER JOIN general_hospital.departments AS d
ON e.department_id = d.department_id
;
SELECT
     *
FROM general_hospital.encounters_h111000
;
CREATE INDEX ON general_hospital.encounters_partitioned (patient_encounter_id)
;



CREATE TABLE general_hospital.vitals_partitioned (
     patient_encounter_id int NOT NULL REFERENCES general_hospital.encounters (patient_encounter_id)
    ,collection_datetime timestamp NOT NULL
    ,bp_diastolic int
    ,bp_systolic int
    ,bmi numeric
    ,temperature numeric
    ,weight int
) PARTITION BY RANGE (collection_datetime)
;
CREATE TABLE IF NOT EXISTS general_hospital.vitals_y2015
    PARTITION OF general_hospital.vitals_partitioned
    FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS general_hospital.vitals_y2016
    PARTITION OF general_hospital.vitals_partitioned
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS general_hospital.vitals_y2017
    PARTITION OF general_hospital.vitals_partitioned
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS general_hospital.vitals_default
    PARTITION OF general_hospital.vitals_partitioned
    DEFAULT;
INSERT INTO general_hospital.vitals_partitioned
SELECT
     e.patient_encounter_id
    ,e.patient_admission_datetime AS collection_datetime
    ,v.bp_diastolic
    ,v.bp_systolic
    ,v.bmi
    ,v.temperature
    ,v.weight
FROM general_hospital.vitals AS v
LEFT OUTER JOIN general_hospital.encounters AS e
ON v.patient_encounter_id = e.patient_encounter_id
;
SELECT
     DISTINCT EXTRACT(YEAR FROM collection_datetime)
FROM general_hospital.vitals_y2016
;



SELECT *
FROM information_schema.views
WHERE table_schema = 'general_hospital'
;

DROP VIEW IF EXISTS general_hospital.v_monthly_surgery_stats_by_department;
CREATE VIEW general_hospital.v_monthly_surgery_stats_by_department AS
SELECT
     TO_CHAR(surgical_admission_date, 'YYYY-MM') AS surgical_admission_date
    ,unit_name
    ,COUNT(surgery_id) AS num_surgieries
    ,SUM(total_cost) AS total_cost
    ,SUM(total_profit) AS total_profit
FROM general_hospital.surgical_encounters
GROUP BY
    TO_CHAR(surgical_admission_date, 'YYYY-MM')
    ,unit_name
ORDER BY
     unit_name
    ,TO_CHAR(surgical_admission_date, 'YYYY-MM')
;
SELECT *
FROM general_hospital.v_monthly_surgery_stats_by_department
;


CREATE OR REPLACE VIEW general_hospital.v_monthly_surgery_stats AS
SELECT
     TO_CHAR(surgical_admission_date, 'YYYY-MM') AS year_month
    ,COUNT(surgery_id) AS num_surgeries
    ,SUM(total_cost) AS total_cost
    ,SUM(total_profit) AS total_profit
FROM general_hospital.surgical_encounters
GROUP BY
    1
ORDER BY
    1
;
ALTER VIEW IF EXISTS general_hospital.v_monthly_surgery_stats
    RENAME TO view_monthly_surgery_stats
;


SELECT
     department_id
    ,COUNT(*)
FROM general_hospital.encounters
GROUP BY
    1
ORDER BY
    1
;
DROP VIEW IF EXISTS general_hospital.v_encounters_department_22100005;
CREATE VIEW general_hospital.v_encounters_department_22100005 AS
SELECT
     patient_encounter_id
    ,admitting_provider_id
    ,department_id
    ,patient_in_icu_flag
FROM general_hospital.encounters
WHERE
    department_id = 22100005
;
INSERT INTO general_hospital.v_encounters_department_22100005 VALUES
    (12345, 5611, 22100006, 'Yes');
CREATE OR REPLACE VIEW general_hospital.v_encounters_department_22100005 AS
SELECT
     patient_encounter_id
    ,admitting_provider_id
    ,department_id
    ,patient_in_icu_flag
FROM general_hospital.encounters
WHERE department_id = 22100005
WITH CHECK OPTION
;
SELECT *
FROM general_hospital.v_encounters_department_22100005
;
UPDATE general_hospital.v_encounters_department_22100005
    SET department_id = 22100006
    WHERE patient_encounter_id = 4915064;

CREATE MATERIALIZED VIEW general_hospital.v_monthly_surgery_stats AS
SELECT
     TO_CHAR(surgical_admission_date, 'YYYY-MM')
    ,unit_name
    ,COUNT(surgery_id) AS num_surgeries
    ,SUM(total_cost) AS total_cost
    ,SUM(total_profit) AS total_profit
FROM general_hospital.surgical_encounters
GROUP BY
     1
    ,2
ORDER BY
     1
    ,2
WITH NO DATA
;
REFRESH MATERIALIZED VIEW general_hospital.v_monthly_surgery_stats
;
ALTER MATERIALIZED VIEW general_hospital.v_monthly_surgery_stats
    RENAME TO mv_monthly_surgery_stats
;
ALTER MATERIALIZED VIEW general_hospital.mv_monthly_surgery_stats
    RENAME COLUMN to_char TO yaer_month
;



CREATE RECURSIVE VIEW v_fibonacci(a, b) AS
        SELECT
             1 AS a
            ,1 AS b
    UNION ALL
        SELECT
             b
            ,a + b
        FROM v_fibonacci
        WHERE b < 200
;


CREATE RECURSIVE VIEW general_hospital.v_orders(order_procedure_id, order_parent_order_id, level) AS
            SELECT
                 order_procedure_id
                ,order_parent_order_id
                ,0 As level
            FROM general_hospital.orders_procedures
            WHERE order_parent_order_id IS NULL
        
        UNION ALL
        
            SELECT
                 op.order_procedure_id
                ,op.order_parent_order_id
                ,level + 1 AS level
            FROM general_hospital.orders_procedures AS op
            INNER JOIN v_orders AS o
            ON op.order_parent_order_id = o.order_procedure_id
;
SELECT
     *
FROM general_hospital.v_orders
WHERE order_parent_order_id IS NOT NULL
;



DROP VIEW IF EXISTS general_hospital.v_patients_primary_care;
CREATE VIEW general_hospital.v_patients_primary_care AS
    SELECT
         p.master_patient_id
         ,p.name AS patient_name
         ,p.gender
         ,p.primary_language
         ,p.date_of_birth
         ,p.pcp_id
         ,ph.full_name AS pcp_name
FROM general_hospital.patients AS p
LEFT OUTER JOIN general_hospital.physicians AS ph
ON p.pcp_id = ph.id
;



CREATE MATERIALIZED VIEW general_hospital.mv_hospital_encounters AS
SELECT
     h.hospital_id
    ,h.hospital_name
    ,TO_CHAR(patient_admission_datetime, 'YYYY-MM') AS year_month
    ,COUNT(patient_encounter_id) AS num_encounters
    ,COUNT(NULLIF(patient_in_icu_flag, 'N')) AS num_icu_patients
FROM general_hospital.encounters AS e
LEFT OUTER JOIN general_hospital.departments AS d
    ON e.department_id = d.department_id
LEFT OUTER JOIN general_hospital.hospitals AS h
    ON d.hospital_id = h.hospital_id
GROUP BY
     1
    ,2
    ,3
ORDER BY
     1
    ,2
    ,3
WITH NO DATA
;
REFRESH MATERIALIZED VIEW general_hospital.mv_hospital_encounters;
SELECT * FROM general_hospital.mv_hospital_encounters;
ALTER MATERIALIZED VIEW general_hospital.mv_hospital_encounters
    RENAME TO mv_hospital_encounters_statistics
;



DROP MATERIALIZED VIEW general_hospital.v_patients_primary_malhem;
CREATE MATERIALIZED VIEW general_hospital.v_patients_primary_malhem AS
SELECT
     p.master_patient_id
    ,p.name AS patient_name
    ,p.gender
    ,p.primary_language
    ,p.pcp_id
    ,p.date_of_birth
FROM general_hospital.patients AS p
WHERE
    p.pcp_id = 4121
WITH CHECK OPTION
;
ALTER MATERIALIZED VIEW general_hospital.v_patients_primary_malhem
    ALTER COLUMN pcp_id SET DEFAULT 4121
;
ALTER VIEW general_hospital.v_patients_primary_malhem
    RENAME TO v_patients_primary_care_malhem
;
INSERT INTO general_hospital.v_patients_primary_care_malhem VALUES
    (1245, 'John Dee', 'Male', 'ENGLISH', 4122, '2003-07-09')
;


CREATE FUNCTION general_hospital.f_test_function(a int, b int)
    RETURNS int
    LANGUAGE SQL
    AS
    'SELECT $1 + $2;';

SELECT general_hospital.f_test_function(2, 2);

CREATE FUNCTION general_hospital.f_plpgsql_function(a int, b int)
    RETURNS int
    AS $$
    BEGIN
        RETURN a + b;
    END;
    $$ LANGUAGE plpgsql;
SELECT general_hospital.f_plpgsql_function(1, 2);
SELECT general_hospital.f_plpgsql_function(a => 1, b=>2);



CREATE FUNCTION general_hospital.f_calculate_los(start_time timestamp, end_time timestamp)
    RETURNS numeric
    AS $$
    BEGIN
        RETURN ROUND((EXTRACT(EPOCH FROM (end_time - start_time))/3600)::numeric, 2);
    END;
    $$ LANGUAGE plpgsql;

SELECT
     patient_admission_datetime
    ,patient_discharge_datetime
    ,general_hospital.f_calculate_los(patient_admission_datetime, patient_discharge_datetime) AS los
FROM general_hospital.encounters AS e;
SELECT
     routine_definition
FROM information_schema.routines
WHERE
    routine_schema = 'general_hospital';


CREATE OR REPLACE FUNCTION general_hospital.f_calculate_los(start_time timestamp, end_time timestamp)
    RETURNS numeric
    AS $$
    BEGIN
        RETURN ROUND(
            (EXTRACT(
                EPOCH FROM (end_time - start_time)
            )/3600)::numeric, 4
        );
    END;
    $$ LANGUAGE plpgsql;
SELECT
    general_hospital.f_calculate_los(patient_admission_datetime, patient_discharge_datetime) AS los
FROM general_hospital.encounters AS e
;
DROP FUNCTION IF EXISTS general_hospital.f_test_function;


CREATE FUNCTION general_hospital.f_mask_field(field text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
    BEGIN
        RETURN md5(field);
    END;
    $$;

SELECT
     name
    ,general_hospital.f_mask_field(name) AS masked_name
FROM general_hospital.patients;

CREATE OR REPLACE FUNCTION general_hospital.f_mask_field(field text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF field IS null THEN
            RETURN null;
        ELSE
            RETURN CONCAT('Patient ', LEFT(md5(field), 8));
        END IF;
    END;
    $$;
SELECT
     name
    ,general_hospital.f_mask_field(name)
FROM general_hospital.patients
ALTER FUNCTION general_hospital.f_mask_field
    RENAME TO f_mask_patient_name;


CREATE PROCEDURE general_hospital.sp_test_procedure()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        DROP TABLE IF EXISTS general_hospital.test_table;
        CREATE TABLE general_hospital.test_table (
            id int
        );
        COMMIT;
    END;
    $$;
CALL general_hospital.sp_test_procedure();
SELECT *
FROM information_schema.routines
WHERE
    ROUTINE_SCHEMA = 'general_hospital'
    AND routine_type = 'PROCEDURE'
;


CREATE OR REPLACE PROCEDURE general_hospital.sp_test_procedure()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        DROP TABLE IF EXISTS general_hospital.test_table_new;
        CREATE TABLE general_hospital.test_table_new (
            id int
        );
        COMMIT;
    END;
    $$;
CALL general_hospital.sp_test_procedure();
ALTER PROCEDURE general_hospital.sp_test_procedure
    SET SCHEMA PUBLIC;
DROP PROCEDURE IF EXISTS general_hospital.sp_test_procedure;



CREATE OR REPLACE PROCEDURE general_hospital.sp_update_surgery_cost(surgery_to_update int, cost_change numeric)
    LANGUAGE plpgsql
    AS $$
    DECLARE
        num_resources int;
    BEGIN
        -- Update surgical encounters
        UPDATE general_hospital.surgical_encounters
            SET total_cost = total_cost + cost_change
            WHERE surgery_id = surgery_to_update;
        COMMIT;
        -- Get number of resources
        SELECT COUNT(*) INTO num_resources
        FROM general_hospital.surgical_costs
        WHERE surgery_id = surgery_to_update;
        -- Update costs table
        UPDATE general_hospital.surgical_costs
            SET resource_cost = resource_cost + (cost_change / num_resources)
            WHERE surgery_id = surgery_to_update;
        COMMIT;
    END;
    $$;
SELECT
     SUM(resource_cost)
FROM general_hospital.surgical_costs
WHERE surgery_id = 6518;
CALL general_hospital.sp_update_surgery_cost(6518, 1000);
