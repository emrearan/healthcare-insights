CREATE TABLE visits (
    visit_date DATE NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('source_a', 'source_b')),
    patient_id VARCHAR(20) NOT NULL,
    visit_id VARCHAR(20) NOT NULL,
    provider_notes_text TEXT,
    provider_notes_author VARCHAR(50) NOT NULL,
    PRIMARY KEY (visit_id, visit_date, source)
) PARTITION BY RANGE (visit_date);

CREATE TABLE visits_20230501 PARTITION OF visits
    FOR VALUES FROM ('2023-05-01') TO ('2023-05-02')
    PARTITION BY LIST (source);

CREATE TABLE visits_20230501_source_a PARTITION OF visits_20230501
    FOR VALUES IN ('source_a');

CREATE TABLE visits_20230501_source_b PARTITION OF visits_20230501
    FOR VALUES IN ('source_b');

CREATE TABLE visits_20230502 PARTITION OF visits
    FOR VALUES FROM ('2023-05-02') TO ('2023-05-03')
    PARTITION BY LIST (source);

CREATE TABLE visits_20230502_source_a PARTITION OF visits_20230502
    FOR VALUES IN ('source_a');

CREATE TABLE visits_20230502_source_b PARTITION OF visits_20230502
    FOR VALUES IN ('source_b');


CREATE INDEX index_visits ON visits (visit_id, patient_id);
