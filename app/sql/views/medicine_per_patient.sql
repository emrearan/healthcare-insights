CREATE VIEW patient_treatment_summary AS
WITH latest_data_of_patients AS (
    SELECT *
    FROM patients
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date)
        FROM patients
    )
)
SELECT
    A.treatments_drug,
    A.treatments_dose,
    A.visit_date AS prescribed_date,
    C.name
FROM treatments A
LEFT JOIN visits B
    ON A.visit_date = B.visit_date
    AND A.visit_id = B.visit_id
LEFT JOIN latest_data_of_patients C
    ON B.patient_id = C.patient_id;
