CREATE TABLE patients (
    patient_id VARCHAR(20) NOT NULL,
    name TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('source_a', 'source_b')),
    created_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (patient_id, snapshot_date, source)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE patients_20230501 PARTITION OF patients
    FOR VALUES FROM ('2023-05-01') TO ('2023-05-02')
    PARTITION BY LIST (source);

CREATE TABLE patients_20230501_source_a PARTITION OF patients_20230501
    FOR VALUES IN ('source_a');

CREATE TABLE patients_20230501_source_b PARTITION OF patients_20230501
    FOR VALUES IN ('source_b');

CREATE TABLE patients_20230502 PARTITION OF patients
    FOR VALUES FROM ('2023-05-02') TO ('2023-05-03')
    PARTITION BY LIST (source);

CREATE TABLE patients_20230502_source_a PARTITION OF patients_20230502
    FOR VALUES IN ('source_a');

CREATE TABLE patients_20230502_source_b PARTITION OF patients_20230502
    FOR VALUES IN ('source_b');

CREATE INDEX index_patients ON patients (patient_id);
