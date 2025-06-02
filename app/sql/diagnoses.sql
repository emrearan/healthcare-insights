CREATE TABLE diagnoses (
    visit_date DATE NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('source_a', 'source_b')),
    visit_id VARCHAR(20) NOT NULL,
    diagnosis_code VARCHAR(20) NOT NULL,
    diagnosis_description TEXT,
    PRIMARY KEY (diagnosis_code, visit_date, source)
) PARTITION BY RANGE (visit_date);

CREATE TABLE diagnoses_20230501 PARTITION OF diagnoses
    FOR VALUES FROM ('2023-05-01') TO ('2023-05-02')
    PARTITION BY LIST (source);

CREATE TABLE diagnoses_20230501_source_a PARTITION OF diagnoses_20230501
    FOR VALUES IN ('source_a');

CREATE TABLE diagnoses_20230501_source_b PARTITION OF diagnoses_20230501
    FOR VALUES IN ('source_b');

CREATE TABLE diagnoses_20230502 PARTITION OF diagnoses
    FOR VALUES FROM ('2023-05-02') TO ('2023-05-03')
    PARTITION BY LIST (source);

CREATE TABLE diagnoses_20230502_source_a PARTITION OF diagnoses_20230502
    FOR VALUES IN ('source_a');

CREATE TABLE diagnoses_20230502_source_b PARTITION OF diagnoses_20230502
    FOR VALUES IN ('source_b');

CREATE INDEX index_diagnoses ON diagnoses (diagnosis_code, visit_id);
