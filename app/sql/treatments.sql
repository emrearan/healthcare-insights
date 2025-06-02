CREATE TABLE treatments (
    visit_date DATE NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('source_a', 'source_b')),
    visit_id VARCHAR(20) NOT NULL,
    diagnosis_code VARCHAR(20) NOT NULL,
    treatments_drug VARCHAR(50) NOT NULL,
    treatments_dose VARCHAR(50) NOT NULL
) PARTITION BY RANGE (visit_date);

CREATE TABLE treatments_20230501 PARTITION OF treatments
    FOR VALUES FROM ('2023-05-01') TO ('2023-05-02')
    PARTITION BY LIST (source);

CREATE TABLE treatments_20230501_source_a PARTITION OF treatments_20230501
    FOR VALUES IN ('source_a');

CREATE TABLE treatments_20230501_source_b PARTITION OF treatments_20230501
    FOR VALUES IN ('source_b');

CREATE TABLE treatments_20230502 PARTITION OF treatments
    FOR VALUES FROM ('2023-05-02') TO ('2023-05-03')
    PARTITION BY LIST (source);

CREATE TABLE treatments_20230502_source_a PARTITION OF treatments_20230502
    FOR VALUES IN ('source_a');

CREATE TABLE treatments_20230502_source_b PARTITION OF treatments_20230502
    FOR VALUES IN ('source_b');

CREATE INDEX index_treatments ON treatments (treatments_drug, diagnosis_code);
