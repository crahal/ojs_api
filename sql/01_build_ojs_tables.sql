-- Build or advance the temporal, API-facing PKP Beacon article catalogue.
--
-- The caller must set these user variables before sourcing this file:
--   @ojs_snapshot_date         DATE-compatible YYYY-MM-DD
--   @ojs_snapshot_completed_at DATETIME-compatible value
--   @ojs_source_filename       raw dump filename
--   @ojs_source_size_bytes     raw dump size
--   @ojs_source_sha256         raw dump SHA-256
--   @ojs_build_sql_sha256      this file's SHA-256
--   @ojs_mysql_version         server version
--   @ojs_full_rescan           0 for incremental metadata parsing, 1 for all rows
--
-- A prior clean SQL export may be loaded before this script. The tables below
-- are therefore created only when absent, then advanced by exactly one snapshot.

SET SESSION group_concat_max_len = 1073741824;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET SESSION sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

CREATE TABLE IF NOT EXISTS ojs_snapshots (
    snapshot_date DATE NOT NULL,
    dump_completed_at DATETIME NOT NULL,
    source_filename VARCHAR(255) NOT NULL,
    source_size_bytes BIGINT UNSIGNED NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    build_sql_sha256 CHAR(64) NOT NULL,
    mysql_version VARCHAR(128) NOT NULL,
    full_metadata_rescan TINYINT(1) NOT NULL,
    source_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    active_source_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    article_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    active_article_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    removed_article_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    merged_article_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS ojs_articles (
    article_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(16) NOT NULL,
    merged_into_article_id BIGINT UNSIGNED NULL,
    canonical_source_record_id BIGINT UNSIGNED NULL,
    source_count BIGINT UNSIGNED NOT NULL,
    active_source_count BIGINT UNSIGNED NOT NULL,
    version_number INT UNSIGNED NOT NULL,
    date_added DATE NOT NULL,
    date_modified DATE NOT NULL,
    date_removed DATE NULL,
    application VARCHAR(32) NULL,
    journal_issn CHAR(8) NULL,
    journal_title VARCHAR(512) NULL,
    endpoint_oai_url VARCHAR(2048) NULL,
    source_oai_identifier VARCHAR(1024) NULL,
    record_update_date DATETIME NULL,
    record_publish_date DATETIME NULL,
    title TEXT NULL,
    creators MEDIUMTEXT NULL,
    subjects MEDIUMTEXT NULL,
    description MEDIUMTEXT NULL,
    publisher TEXT NULL,
    published TEXT NULL,
    types TEXT NULL,
    formats TEXT NULL,
    identifiers MEDIUMTEXT NULL,
    source_title TEXT NULL,
    languages TEXT NULL,
    relations MEDIUMTEXT NULL,
    coverage TEXT NULL,
    rights MEDIUMTEXT NULL,
    doi VARCHAR(255) NULL,
    article_url VARCHAR(2048) NULL,
    metadata_xml MEDIUMTEXT NULL,
    canonical_metadata_hash BINARY(32) NULL,
    data_hash BINARY(32) NOT NULL,
    provenance_hash BINARY(32) NOT NULL,
    PRIMARY KEY (article_id),
    INDEX idx_ojs_articles_status_id (status, article_id),
    INDEX idx_ojs_articles_modified_id (date_modified, article_id),
    INDEX idx_ojs_articles_removed_id (date_removed, article_id),
    INDEX idx_ojs_articles_doi (doi),
    INDEX idx_ojs_articles_issn_id (journal_issn, article_id),
    INDEX idx_ojs_articles_merged_into (merged_into_article_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS ojs_article_sources (
    source_record_id BIGINT UNSIGNED NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    context_id BIGINT UNSIGNED NOT NULL,
    endpoint_id BIGINT UNSIGNED NOT NULL,
    source_identifier BIGINT UNSIGNED NOT NULL,
    application VARCHAR(32) NULL,
    journal_issn CHAR(8) NULL,
    journal_title VARCHAR(512) NULL,
    endpoint_oai_url VARCHAR(2048) NULL,
    source_oai_identifier VARCHAR(1024) NULL,
    record_update_date DATETIME NULL,
    record_publish_date DATETIME NULL,
    record_created_at DATETIME(6) NULL,
    record_modified_at DATETIME(6) NULL,
    source_removed_at DATETIME NULL,
    is_present TINYINT(1) NOT NULL,
    is_active TINYINT(1) NOT NULL,
    date_added DATE NOT NULL,
    date_modified DATE NOT NULL,
    date_removed DATE NULL,
    title TEXT NULL,
    first_creator TEXT NULL,
    publication_year CHAR(4) NULL,
    doi VARCHAR(255) NULL,
    article_url VARCHAR(2048) NULL,
    metadata_hash BINARY(32) NULL,
    oai_key_hash BINARY(32) NULL,
    doi_key_hash BINARY(32) NULL,
    url_key_hash BINARY(32) NULL,
    fingerprint_key_hash BINARY(32) NULL,
    PRIMARY KEY (source_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS ojs_article_keys (
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    key_value VARCHAR(2048) NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    date_added DATE NOT NULL,
    PRIMARY KEY (key_type, key_hash),
    INDEX idx_ojs_keys_article (article_id, key_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS ojs_article_events (
    event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    snapshot_date DATE NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    event_type VARCHAR(16) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    version_number INT UNSIGNED NOT NULL,
    redirect_to_article_id BIGINT UNSIGNED NULL,
    previous_data_hash BINARY(32) NULL,
    data_hash BINARY(32) NULL,
    previous_provenance_hash BINARY(32) NULL,
    provenance_hash BINARY(32) NULL,
    reason JSON NOT NULL,
    PRIMARY KEY (event_id),
    UNIQUE KEY uq_ojs_event_article_snapshot_type (
        article_id,
        snapshot_date,
        event_type
    ),
    INDEX idx_ojs_events_snapshot_id (snapshot_date, event_id),
    INDEX idx_ojs_events_article_id (article_id, event_id),
    INDEX idx_ojs_events_operation_id (operation, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP PROCEDURE IF EXISTS ojs_assert_snapshot;
DELIMITER //
CREATE PROCEDURE ojs_assert_snapshot()
BEGIN
    IF @ojs_snapshot_date IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = '@ojs_snapshot_date must be set by the pipeline';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM ojs_snapshots
        WHERE snapshot_date = CAST(@ojs_snapshot_date AS DATE)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'snapshot has already been applied to the clean catalogue';
    END IF;
END//
DELIMITER ;
CALL ojs_assert_snapshot();
DROP PROCEDURE ojs_assert_snapshot;

SET @ojs_snapshot_date = CAST(@ojs_snapshot_date AS DATE);
SET @ojs_full_rescan = COALESCE(@ojs_full_rescan, 0);

-- Reduce contexts to one deterministic, normalized ISSN. This preserves the
-- project's original scope: records from contexts represented in the ISSN table.
DROP TABLE IF EXISTS ojs_stage_contexts;
CREATE TABLE ojs_stage_contexts (
    context_id BIGINT UNSIGNED NOT NULL,
    endpoint_id BIGINT UNSIGNED NOT NULL,
    application VARCHAR(32) NULL,
    journal_issn CHAR(8) NOT NULL,
    journal_title VARCHAR(512) NULL,
    endpoint_oai_url VARCHAR(2048) NULL,
    PRIMARY KEY (context_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_contexts (
    context_id,
    endpoint_id,
    application,
    journal_issn,
    journal_title,
    endpoint_oai_url
)
SELECT
    c.id,
    c.endpoint_id,
    NULLIF(TRIM(e.application), ''),
    MIN(
        CASE
            WHEN LENGTH(
                REGEXP_REPLACE(UPPER(COALESCE(i.issn, '')), '[^0-9X]', '')
            ) = 8
                THEN REGEXP_REPLACE(
                    UPPER(COALESCE(i.issn, '')),
                    '[^0-9X]',
                    ''
                )
            ELSE NULL
        END
    ) AS journal_issn,
    NULLIF(TRIM(c.name), ''),
    NULLIF(TRIM(e.oai_url), '')
FROM contexts c
INNER JOIN endpoints e
    ON e.id = c.endpoint_id
INNER JOIN issns i
    ON i.context_id = c.id
GROUP BY
    c.id,
    c.endpoint_id,
    e.application,
    c.name,
    e.oai_url
HAVING journal_issn IS NOT NULL;

-- The compact source index is scanned every month. The large XML payload is
-- parsed only for new or changed rows, based on Beacon/OAI timestamps and
-- source-state fields retained from the prior clean snapshot.
DROP TABLE IF EXISTS ojs_stage_source_index;
CREATE TABLE ojs_stage_source_index (
    source_record_id BIGINT UNSIGNED NOT NULL,
    context_id BIGINT UNSIGNED NOT NULL,
    endpoint_id BIGINT UNSIGNED NOT NULL,
    source_identifier BIGINT UNSIGNED NOT NULL,
    application VARCHAR(32) NULL,
    journal_issn CHAR(8) NULL,
    journal_title VARCHAR(512) NULL,
    endpoint_oai_url VARCHAR(2048) NULL,
    record_update_date DATETIME NULL,
    record_publish_date DATETIME NULL,
    record_created_at DATETIME(6) NULL,
    record_modified_at DATETIME(6) NULL,
    source_removed_at DATETIME NULL,
    is_active TINYINT(1) NOT NULL,
    needs_metadata_parse TINYINT(1) NOT NULL,
    PRIMARY KEY (source_record_id),
    INDEX idx_ojs_stage_source_needs_parse (needs_metadata_parse, source_record_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_source_index (
    source_record_id,
    context_id,
    endpoint_id,
    source_identifier,
    application,
    journal_issn,
    journal_title,
    endpoint_oai_url,
    record_update_date,
    record_publish_date,
    record_created_at,
    record_modified_at,
    source_removed_at,
    is_active,
    needs_metadata_parse
)
SELECT
    r.id,
    r.context_id,
    c.endpoint_id,
    r.identifier,
    c.application,
    c.journal_issn,
    c.journal_title,
    c.endpoint_oai_url,
    r.update_date,
    r.publish_date,
    r.created_at,
    r.modified_at,
    r.removed_at,
    r.removed_at IS NULL,
    (
        @ojs_full_rescan = 1
        OR prior.source_record_id IS NULL
        OR prior.is_present = 0
        OR NOT (prior.context_id <=> r.context_id)
        OR NOT (prior.endpoint_id <=> c.endpoint_id)
        OR NOT (prior.source_identifier <=> r.identifier)
        OR NOT (prior.application <=> c.application)
        OR NOT (prior.journal_issn <=> c.journal_issn)
        OR NOT (prior.journal_title <=> c.journal_title)
        OR NOT (prior.endpoint_oai_url <=> c.endpoint_oai_url)
        OR NOT (prior.record_update_date <=> r.update_date)
        OR NOT (prior.record_publish_date <=> r.publish_date)
        OR NOT (prior.record_created_at <=> r.created_at)
        OR NOT (prior.record_modified_at <=> r.modified_at)
        OR NOT (prior.source_removed_at <=> r.removed_at)
        OR prior.is_active <> (r.removed_at IS NULL)
    )
FROM records r FORCE INDEX (records_context_id_identifier_unique)
INNER JOIN ojs_stage_contexts c
    ON c.context_id = r.context_id
LEFT JOIN ojs_article_sources prior
    ON prior.source_record_id = r.id;

-- A Beacon record ID is the durable source-row identity. Reusing an existing
-- ID for a different context/endpoint/source identifier would silently attach
-- unrelated metadata to a published article, so quarantine that snapshot
-- instead of guessing which identity should win.
DROP PROCEDURE IF EXISTS ojs_assert_source_identity;
DELIMITER //
CREATE PROCEDURE ojs_assert_source_identity()
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ojs_stage_source_index current_source
        INNER JOIN ojs_article_sources prior
            ON prior.source_record_id = current_source.source_record_id
        WHERE NOT (prior.context_id <=> current_source.context_id)
           OR NOT (prior.endpoint_id <=> current_source.endpoint_id)
           OR NOT (
               prior.source_identifier <=> current_source.source_identifier
           )
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'source record ID was reused with a different identity tuple';
    END IF;
END//
DELIMITER ;
CALL ojs_assert_source_identity();
DROP PROCEDURE ojs_assert_source_identity;

DROP TABLE IF EXISTS ojs_stage_sources;
CREATE TABLE ojs_stage_sources (
    source_record_id BIGINT UNSIGNED NOT NULL,
    context_id BIGINT UNSIGNED NOT NULL,
    endpoint_id BIGINT UNSIGNED NOT NULL,
    source_identifier BIGINT UNSIGNED NOT NULL,
    application VARCHAR(32) NULL,
    journal_issn CHAR(8) NULL,
    journal_title VARCHAR(512) NULL,
    endpoint_oai_url VARCHAR(2048) NULL,
    source_oai_identifier VARCHAR(1024) NULL,
    record_update_date DATETIME NULL,
    record_publish_date DATETIME NULL,
    record_created_at DATETIME(6) NULL,
    record_modified_at DATETIME(6) NULL,
    source_removed_at DATETIME NULL,
    is_active TINYINT(1) NOT NULL,
    title TEXT NULL,
    creators MEDIUMTEXT NULL,
    first_creator TEXT NULL,
    subjects MEDIUMTEXT NULL,
    description MEDIUMTEXT NULL,
    publisher TEXT NULL,
    published TEXT NULL,
    publication_year CHAR(4) NULL,
    types TEXT NULL,
    formats TEXT NULL,
    identifiers MEDIUMTEXT NULL,
    source_title TEXT NULL,
    languages TEXT NULL,
    relations MEDIUMTEXT NULL,
    coverage TEXT NULL,
    rights MEDIUMTEXT NULL,
    doi VARCHAR(255) NULL,
    article_url VARCHAR(2048) NULL,
    url_key_value VARCHAR(2048) NULL,
    fingerprint_key_value VARCHAR(2048) NULL,
    oai_key_value VARCHAR(2048) NULL,
    metadata_xml MEDIUMTEXT NULL,
    metadata_hash BINARY(32) NULL,
    oai_key_hash BINARY(32) NULL,
    doi_key_hash BINARY(32) NULL,
    url_key_hash BINARY(32) NULL,
    fingerprint_key_hash BINARY(32) NULL,
    PRIMARY KEY (source_record_id)
) ENGINE=InnoDB;

-- OJS_PIPELINE_METADATA_BEGIN
INSERT INTO ojs_stage_sources (
    source_record_id,
    context_id,
    endpoint_id,
    source_identifier,
    application,
    journal_issn,
    journal_title,
    endpoint_oai_url,
    source_oai_identifier,
    record_update_date,
    record_publish_date,
    record_created_at,
    record_modified_at,
    source_removed_at,
    is_active,
    title,
    creators,
    first_creator,
    subjects,
    description,
    publisher,
    published,
    publication_year,
    types,
    formats,
    identifiers,
    source_title,
    languages,
    relations,
    coverage,
    rights,
    doi,
    article_url,
    url_key_value,
    fingerprint_key_value,
    oai_key_value,
    metadata_xml,
    metadata_hash,
    oai_key_hash,
    doi_key_hash,
    url_key_hash,
    fingerprint_key_hash
)
SELECT
    normalized.source_record_id,
    normalized.context_id,
    normalized.endpoint_id,
    normalized.source_identifier,
    normalized.application,
    normalized.journal_issn,
    normalized.journal_title,
    normalized.endpoint_oai_url,
    normalized.source_oai_identifier,
    normalized.record_update_date,
    normalized.record_publish_date,
    normalized.record_created_at,
    normalized.record_modified_at,
    normalized.source_removed_at,
    normalized.is_active,
    normalized.title,
    normalized.creators,
    normalized.first_creator,
    normalized.subjects,
    normalized.description,
    normalized.publisher,
    normalized.published,
    normalized.publication_year,
    normalized.types,
    normalized.formats,
    normalized.identifiers,
    normalized.source_title,
    normalized.languages,
    normalized.relations,
    normalized.coverage,
    normalized.rights,
    normalized.doi,
    normalized.article_url,
    normalized.url_key_value,
    normalized.fingerprint_key_value,
    normalized.oai_key_value,
    normalized.metadata_xml,
    UNHEX(SHA2(normalized.metadata_xml, 256)),
    CASE
        WHEN normalized.oai_key_value IS NULL THEN NULL
        ELSE UNHEX(SHA2(CONCAT('oai:', normalized.oai_key_value), 256))
    END,
    CASE
        WHEN normalized.doi IS NULL THEN NULL
        ELSE UNHEX(SHA2(CONCAT('doi:', normalized.doi), 256))
    END,
    CASE
        WHEN normalized.url_key_value IS NULL THEN NULL
        ELSE UNHEX(SHA2(CONCAT('url:', normalized.url_key_value), 256))
    END,
    CASE
        WHEN normalized.fingerprint_key_value IS NULL THEN NULL
        ELSE UNHEX(
            SHA2(
                CONCAT('fingerprint:', normalized.fingerprint_key_value),
                256
            )
        )
    END
FROM (
    SELECT
        keyed.*,
        CASE
            WHEN keyed.article_url IS NULL THEN NULL
            ELSE NULLIF(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            LOWER(keyed.article_url),
                            '^https?://(www\\.)?',
                            ''
                        ),
                        '[?#].*$',
                        ''
                    ),
                    '/+$',
                    ''
                ),
                ''
            )
        END AS url_key_value,
        CASE
            WHEN CHAR_LENGTH(keyed.title_key) >= 20
             AND keyed.first_creator_key IS NOT NULL
             AND keyed.publication_year IS NOT NULL
                THEN LEFT(
                    CONCAT(
                        keyed.title_key,
                        '|',
                        keyed.first_creator_key,
                        '|',
                        keyed.publication_year
                    ),
                    2048
                )
            ELSE NULL
        END AS fingerprint_key_value,
        CASE
            WHEN keyed.source_oai_identifier IS NULL
              OR keyed.endpoint_key IS NULL
                THEN NULL
            ELSE LEFT(
                CONCAT(
                    keyed.endpoint_key,
                    '|',
                    LOWER(keyed.source_oai_identifier)
                ),
                2048
            )
        END AS oai_key_value
    FROM (
        SELECT
            extracted.*,
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        REGEXP_REPLACE(
                            LOWER(COALESCE(extracted.title, '')),
                            '[^[:alnum:]]+',
                            ' '
                        )
                    ),
                    '[[:space:]]+',
                    ' '
                ),
                ''
            ) AS title_key,
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        REGEXP_REPLACE(
                            LOWER(COALESCE(extracted.first_creator, '')),
                            '[^[:alnum:]]+',
                            ' '
                        )
                    ),
                    '[[:space:]]+',
                    ' '
                ),
                ''
            ) AS first_creator_key,
            REGEXP_SUBSTR(extracted.published, '[12][0-9]{3}') AS publication_year,
            LOWER(
                LEFT(
                    REGEXP_REPLACE(
                        REGEXP_SUBSTR(
                            CONCAT_WS(
                                ' ',
                                extracted.identifiers,
                                extracted.relations
                            ),
                            '10\\.[0-9]{4,9}/[-._;()/:[:alnum:]]+',
                            1,
                            1,
                            'i'
                        ),
                        '[.,;]+$',
                        ''
                    ),
                    255
                )
            ) AS doi,
            LEFT(
                REGEXP_REPLACE(
                    REGEXP_SUBSTR(
                        CONCAT_WS(
                            ' ',
                            extracted.identifiers,
                            extracted.relations
                        ),
                        'https?://[^[:space:]<>"'']+/(article/view|catalog/book)/[^[:space:]<>"'']*',
                        1,
                        1,
                        'i'
                    ),
                    '[.,;>]+$',
                    ''
                ),
                2048
            ) AS article_url,
            NULLIF(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        LOWER(COALESCE(extracted.endpoint_oai_url, '')),
                        '^https?://(www\\.)?',
                        ''
                    ),
                    '/+$',
                    ''
                ),
                ''
            ) AS endpoint_key
        FROM (
            SELECT
                i.source_record_id,
                i.context_id,
                i.endpoint_id,
                i.source_identifier,
                i.application,
                i.journal_issn,
                i.journal_title,
                i.endpoint_oai_url,
                LEFT(
                    NULLIF(
                        ExtractValue(r.metadata, '//header/identifier'),
                        ''
                    ),
                    1024
                ) AS source_oai_identifier,
                i.record_update_date,
                i.record_publish_date,
                i.record_created_at,
                i.record_modified_at,
                i.source_removed_at,
                i.is_active,
                NULLIF(ExtractValue(r.metadata, '//dc:title'), '') AS title,
                NULLIF(ExtractValue(r.metadata, '//dc:creator'), '') AS creators,
                NULLIF(ExtractValue(r.metadata, '//dc:creator[1]'), '') AS first_creator,
                NULLIF(ExtractValue(r.metadata, '//dc:subject'), '') AS subjects,
                NULLIF(ExtractValue(r.metadata, '//dc:description'), '') AS description,
                NULLIF(ExtractValue(r.metadata, '//dc:publisher'), '') AS publisher,
                NULLIF(ExtractValue(r.metadata, '//dc:date'), '') AS published,
                NULLIF(ExtractValue(r.metadata, '//dc:type'), '') AS types,
                NULLIF(ExtractValue(r.metadata, '//dc:format'), '') AS formats,
                NULLIF(ExtractValue(r.metadata, '//dc:identifier'), '') AS identifiers,
                NULLIF(ExtractValue(r.metadata, '//dc:source'), '') AS source_title,
                NULLIF(ExtractValue(r.metadata, '//dc:language'), '') AS languages,
                NULLIF(ExtractValue(r.metadata, '//dc:relation'), '') AS relations,
                NULLIF(ExtractValue(r.metadata, '//dc:coverage'), '') AS coverage,
                NULLIF(ExtractValue(r.metadata, '//dc:rights'), '') AS rights,
                r.metadata AS metadata_xml
            FROM ojs_stage_source_index i
            INNER JOIN records r
                ON r.id = i.source_record_id
            WHERE i.needs_metadata_parse = 1
              AND i.source_record_id BETWEEN
                  @ojs_metadata_min_id AND @ojs_metadata_max_id
        ) extracted
    ) keyed
) normalized;
-- OJS_PIPELINE_METADATA_END

-- Materialize all available exact keys. High-frequency blocks are discarded
-- before label propagation to prevent a malformed metadata value from creating
-- a giant false-positive cluster.
DROP TABLE IF EXISTS ojs_stage_keys;
CREATE TABLE ojs_stage_keys (
    source_record_id BIGINT UNSIGNED NOT NULL,
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    key_value VARCHAR(2048) NOT NULL,
    PRIMARY KEY (source_record_id, key_type),
    INDEX idx_ojs_stage_keys_lookup (key_type, key_hash, source_record_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_keys
SELECT source_record_id, 'oai', oai_key_hash, oai_key_value
FROM ojs_stage_sources
WHERE oai_key_hash IS NOT NULL
UNION ALL
SELECT source_record_id, 'doi', doi_key_hash, doi
FROM ojs_stage_sources
WHERE doi_key_hash IS NOT NULL
UNION ALL
SELECT source_record_id, 'url', url_key_hash, url_key_value
FROM ojs_stage_sources
WHERE url_key_hash IS NOT NULL
UNION ALL
SELECT
    source_record_id,
    'fingerprint',
    fingerprint_key_hash,
    fingerprint_key_value
FROM ojs_stage_sources
WHERE fingerprint_key_hash IS NOT NULL;

-- Reconstruct the effective key membership from current staged replacements
-- plus every retained alias. Staged rows replace (rather than append to) their
-- prior keys, which retires corrected DOI/OAI/URL/fingerprint values from all
-- future candidate matching without attempting to split an established merge.
DROP TABLE IF EXISTS ojs_stage_effective_keys;
CREATE TABLE ojs_stage_effective_keys (
    source_record_id BIGINT UNSIGNED NOT NULL,
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    key_value VARCHAR(2048) NULL,
    article_id BIGINT UNSIGNED NULL,
    is_present TINYINT(1) NOT NULL,
    PRIMARY KEY (source_record_id, key_type),
    INDEX idx_ojs_effective_keys_lookup (
        key_type,
        key_hash,
        article_id,
        source_record_id
    )
) ENGINE=InnoDB;

INSERT INTO ojs_stage_effective_keys
SELECT
    staged_key.source_record_id,
    staged_key.key_type,
    staged_key.key_hash,
    staged_key.key_value,
    prior.article_id,
    1
FROM ojs_stage_keys staged_key
LEFT JOIN ojs_article_sources prior
    ON prior.source_record_id = staged_key.source_record_id;

INSERT INTO ojs_stage_effective_keys
SELECT
    retained.source_record_id,
    retained.key_type,
    retained.key_hash,
    identity_key.key_value,
    retained.article_id,
    retained.is_present
FROM (
    SELECT
        prior.source_record_id,
        'oai' AS key_type,
        prior.oai_key_hash AS key_hash,
        prior.article_id,
        current_source.source_record_id IS NOT NULL AS is_present
    FROM ojs_article_sources prior
    LEFT JOIN ojs_stage_sources replacement
        ON replacement.source_record_id = prior.source_record_id
    LEFT JOIN ojs_stage_source_index current_source
        ON current_source.source_record_id = prior.source_record_id
    WHERE replacement.source_record_id IS NULL
      AND prior.oai_key_hash IS NOT NULL
    UNION ALL
    SELECT
        prior.source_record_id,
        'doi',
        prior.doi_key_hash,
        prior.article_id,
        current_source.source_record_id IS NOT NULL
    FROM ojs_article_sources prior
    LEFT JOIN ojs_stage_sources replacement
        ON replacement.source_record_id = prior.source_record_id
    LEFT JOIN ojs_stage_source_index current_source
        ON current_source.source_record_id = prior.source_record_id
    WHERE replacement.source_record_id IS NULL
      AND prior.doi_key_hash IS NOT NULL
    UNION ALL
    SELECT
        prior.source_record_id,
        'url',
        prior.url_key_hash,
        prior.article_id,
        current_source.source_record_id IS NOT NULL
    FROM ojs_article_sources prior
    LEFT JOIN ojs_stage_sources replacement
        ON replacement.source_record_id = prior.source_record_id
    LEFT JOIN ojs_stage_source_index current_source
        ON current_source.source_record_id = prior.source_record_id
    WHERE replacement.source_record_id IS NULL
      AND prior.url_key_hash IS NOT NULL
    UNION ALL
    SELECT
        prior.source_record_id,
        'fingerprint',
        prior.fingerprint_key_hash,
        prior.article_id,
        current_source.source_record_id IS NOT NULL
    FROM ojs_article_sources prior
    LEFT JOIN ojs_stage_sources replacement
        ON replacement.source_record_id = prior.source_record_id
    LEFT JOIN ojs_stage_source_index current_source
        ON current_source.source_record_id = prior.source_record_id
    WHERE replacement.source_record_id IS NULL
      AND prior.fingerprint_key_hash IS NOT NULL
) retained
LEFT JOIN ojs_article_keys identity_key
    ON identity_key.key_type = retained.key_type
   AND identity_key.key_hash = retained.key_hash;

DROP TABLE IF EXISTS ojs_stage_noisy_keys;
CREATE TABLE ojs_stage_noisy_keys (
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    PRIMARY KEY (key_type, key_hash)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_noisy_keys
SELECT
    key_type,
    key_hash
FROM ojs_stage_effective_keys
WHERE is_present = 1
GROUP BY key_type, key_hash
HAVING
    (key_type = 'fingerprint' AND COUNT(*) > 25)
    OR (key_type <> 'fingerprint' AND COUNT(*) > 1000);

-- Remember every new, corrected, retired, or newly noisy key so the compact
-- persistent lookup can be reconciled after source assignments are committed.
-- Keeping the original date/value where possible makes that reconciliation
-- stable and avoids converting the lookup itself into an append-only history.
DROP TABLE IF EXISTS ojs_stage_touched_keys;
CREATE TABLE ojs_stage_touched_keys (
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    key_value VARCHAR(2048) NULL,
    date_added DATE NULL,
    PRIMARY KEY (key_type, key_hash)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_touched_keys
SELECT
    effective.key_type,
    effective.key_hash,
    MAX(COALESCE(identity_key.key_value, effective.key_value)),
    MIN(identity_key.date_added)
FROM ojs_stage_effective_keys effective
INNER JOIN ojs_stage_sources staged
    ON staged.source_record_id = effective.source_record_id
LEFT JOIN ojs_article_keys identity_key
    ON identity_key.key_type = effective.key_type
   AND identity_key.key_hash = effective.key_hash
GROUP BY effective.key_type, effective.key_hash;

INSERT INTO ojs_stage_touched_keys
SELECT
    retired.key_type,
    retired.key_hash,
    identity_key.key_value,
    identity_key.date_added
FROM (
    SELECT 'oai' AS key_type, prior.oai_key_hash AS key_hash
    FROM ojs_article_sources prior
    INNER JOIN ojs_stage_sources staged
        ON staged.source_record_id = prior.source_record_id
    WHERE prior.oai_key_hash IS NOT NULL
    UNION
    SELECT 'doi', prior.doi_key_hash
    FROM ojs_article_sources prior
    INNER JOIN ojs_stage_sources staged
        ON staged.source_record_id = prior.source_record_id
    WHERE prior.doi_key_hash IS NOT NULL
    UNION
    SELECT 'url', prior.url_key_hash
    FROM ojs_article_sources prior
    INNER JOIN ojs_stage_sources staged
        ON staged.source_record_id = prior.source_record_id
    WHERE prior.url_key_hash IS NOT NULL
    UNION
    SELECT 'fingerprint', prior.fingerprint_key_hash
    FROM ojs_article_sources prior
    INNER JOIN ojs_stage_sources staged
        ON staged.source_record_id = prior.source_record_id
    WHERE prior.fingerprint_key_hash IS NOT NULL
) retired
LEFT JOIN ojs_article_keys identity_key
    ON identity_key.key_type = retired.key_type
   AND identity_key.key_hash = retired.key_hash
ON DUPLICATE KEY UPDATE
    key_value = COALESCE(
        ojs_stage_touched_keys.key_value,
        VALUES(key_value)
    ),
    date_added = COALESCE(
        LEAST(ojs_stage_touched_keys.date_added, VALUES(date_added)),
        ojs_stage_touched_keys.date_added,
        VALUES(date_added)
    );

INSERT INTO ojs_stage_touched_keys
SELECT
    noisy.key_type,
    noisy.key_hash,
    identity_key.key_value,
    identity_key.date_added
FROM ojs_stage_noisy_keys noisy
LEFT JOIN ojs_article_keys identity_key
    ON identity_key.key_type = noisy.key_type
   AND identity_key.key_hash = noisy.key_hash
ON DUPLICATE KEY UPDATE
    key_value = COALESCE(
        ojs_stage_touched_keys.key_value,
        VALUES(key_value)
    ),
    date_added = COALESCE(
        LEAST(ojs_stage_touched_keys.date_added, VALUES(date_added)),
        ojs_stage_touched_keys.date_added,
        VALUES(date_added)
    );

DELETE k
FROM ojs_stage_keys k
INNER JOIN ojs_stage_noisy_keys noisy
    ON noisy.key_type = k.key_type
   AND noisy.key_hash = k.key_hash;

DROP TABLE IF EXISTS ojs_stage_existing_candidates;
CREATE TABLE ojs_stage_existing_candidates (
    source_record_id BIGINT UNSIGNED NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    candidate_type VARCHAR(24) NOT NULL,
    INDEX idx_ojs_stage_candidates_source (source_record_id, article_id),
    INDEX idx_ojs_stage_candidates_article (article_id, source_record_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_existing_candidates
SELECT
    s.source_record_id,
    s.article_id,
    'source_record'
FROM ojs_stage_sources staged
INNER JOIN ojs_article_sources s
    ON s.source_record_id = staged.source_record_id;

INSERT INTO ojs_stage_existing_candidates
SELECT DISTINCT
    staged_key.source_record_id,
    existing_alias.article_id,
    staged_key.key_type
FROM ojs_stage_keys staged_key
INNER JOIN ojs_stage_effective_keys existing_alias
    ON existing_alias.key_type = staged_key.key_type
   AND existing_alias.key_hash = staged_key.key_hash
WHERE existing_alias.article_id IS NOT NULL;

DROP TABLE IF EXISTS ojs_stage_assignments;
CREATE TABLE ojs_stage_assignments (
    source_record_id BIGINT UNSIGNED NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    has_existing_article_id TINYINT(1) NOT NULL,
    PRIMARY KEY (source_record_id),
    INDEX idx_ojs_stage_assignments_article (article_id, source_record_id)
) ENGINE=InnoDB;

-- The first snapshot has no existing candidates. Avoid an otherwise very
-- expensive LEFT JOIN and GROUP BY over the full baseline in that case.
SET @ojs_has_existing_candidates = EXISTS (
    SELECT 1
    FROM ojs_stage_existing_candidates
    LIMIT 1
);

INSERT INTO ojs_stage_assignments
SELECT
    staged.source_record_id,
    staged.source_record_id,
    0
FROM ojs_stage_sources staged
WHERE @ojs_has_existing_candidates = 0;

INSERT INTO ojs_stage_assignments
SELECT
    staged.source_record_id,
    COALESCE(MIN(candidate.article_id), staged.source_record_id),
    MAX(candidate.article_id IS NOT NULL)
FROM ojs_stage_sources staged
LEFT JOIN ojs_stage_existing_candidates candidate
    ON candidate.source_record_id = staged.source_record_id
WHERE @ojs_has_existing_candidates = 1
GROUP BY staged.source_record_id;

-- Label propagation computes connected components inside the changed batch
-- using indexed key blocks. It never compares unrelated records.
DROP TABLE IF EXISTS ojs_stage_key_labels;
CREATE TABLE ojs_stage_key_labels (
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    has_existing_article_id TINYINT(1) NOT NULL,
    PRIMARY KEY (key_type, key_hash)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS ojs_stage_source_labels;
CREATE TABLE ojs_stage_source_labels (
    source_record_id BIGINT UNSIGNED NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    has_existing_article_id TINYINT(1) NOT NULL,
    PRIMARY KEY (source_record_id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS ojs_stage_frontier_sources;
CREATE TABLE ojs_stage_frontier_sources (
    source_record_id BIGINT UNSIGNED NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    has_existing_article_id TINYINT(1) NOT NULL,
    PRIMARY KEY (source_record_id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS ojs_stage_changed_keys;
CREATE TABLE ojs_stage_changed_keys (
    key_type VARCHAR(16) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    has_existing_article_id TINYINT(1) NOT NULL,
    PRIMARY KEY (key_type, key_hash)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS ojs_stage_next_frontier;
CREATE TABLE ojs_stage_next_frontier (
    source_record_id BIGINT UNSIGNED NOT NULL,
    article_id BIGINT UNSIGNED NOT NULL,
    has_existing_article_id TINYINT(1) NOT NULL,
    PRIMARY KEY (source_record_id)
) ENGINE=InnoDB;

DROP PROCEDURE IF EXISTS ojs_propagate_labels;
DELIMITER //
CREATE PROCEDURE ojs_propagate_labels()
BEGIN
    DECLARE pass_number INT DEFAULT 0;
    DECLARE changed_rows BIGINT DEFAULT 0;

    -- The first pass scans the full key graph once.
    INSERT INTO ojs_stage_key_labels
    SELECT
        k.key_type,
        k.key_hash,
        COALESCE(
            MIN(
                CASE WHEN a.has_existing_article_id = 1
                    THEN a.article_id END
            ),
            MIN(a.article_id)
        ),
        MAX(a.has_existing_article_id)
    FROM ojs_stage_keys k
    INNER JOIN ojs_stage_assignments a
        ON a.source_record_id = k.source_record_id
    GROUP BY k.key_type, k.key_hash;

    INSERT INTO ojs_stage_source_labels
    SELECT
        k.source_record_id,
        COALESCE(
            MIN(
                CASE WHEN labels.has_existing_article_id = 1
                    THEN labels.article_id END
            ),
            MIN(labels.article_id)
        ),
        MAX(labels.has_existing_article_id)
    FROM ojs_stage_keys k
    INNER JOIN ojs_stage_key_labels labels
        ON labels.key_type = k.key_type
       AND labels.key_hash = k.key_hash
    GROUP BY k.source_record_id;

    INSERT INTO ojs_stage_frontier_sources
    SELECT
        next_assignment.source_record_id,
        next_assignment.article_id,
        next_assignment.has_existing_article_id
    FROM ojs_stage_source_labels next_assignment
    INNER JOIN ojs_stage_assignments current_assignment
        ON current_assignment.source_record_id =
           next_assignment.source_record_id
    WHERE
        next_assignment.has_existing_article_id >
            current_assignment.has_existing_article_id
        OR (
            next_assignment.has_existing_article_id =
                current_assignment.has_existing_article_id
            AND next_assignment.article_id < current_assignment.article_id
        );

    UPDATE ojs_stage_assignments current_assignment
    INNER JOIN ojs_stage_frontier_sources next_assignment
        ON next_assignment.source_record_id =
           current_assignment.source_record_id
    SET
        current_assignment.article_id = next_assignment.article_id,
        current_assignment.has_existing_article_id =
            next_assignment.has_existing_article_id;

    SET changed_rows = ROW_COUNT();
    SET pass_number = 1;

    -- Later passes propagate only labels changed in the preceding pass. This
    -- preserves connected-component semantics without rebuilding the complete
    -- key and source label tables each time.
    WHILE pass_number < 64 AND changed_rows > 0 DO
        TRUNCATE TABLE ojs_stage_changed_keys;
        INSERT INTO ojs_stage_changed_keys
        SELECT
            k.key_type,
            k.key_hash,
            COALESCE(
                MIN(
                    CASE WHEN frontier.has_existing_article_id = 1
                        THEN frontier.article_id END
                ),
                MIN(frontier.article_id)
            ) AS next_article_id,
            MAX(frontier.has_existing_article_id) AS next_has_existing_article_id
        FROM ojs_stage_frontier_sources frontier
        INNER JOIN ojs_stage_keys k
            ON k.source_record_id = frontier.source_record_id
        INNER JOIN ojs_stage_key_labels current_label
            ON current_label.key_type = k.key_type
           AND current_label.key_hash = k.key_hash
        GROUP BY k.key_type, k.key_hash
        HAVING
            next_has_existing_article_id >
                MIN(current_label.has_existing_article_id)
            OR (
                next_has_existing_article_id =
                    MIN(current_label.has_existing_article_id)
                AND next_article_id < MIN(current_label.article_id)
            );

        UPDATE ojs_stage_key_labels current_label
        INNER JOIN ojs_stage_changed_keys next_label
            ON next_label.key_type = current_label.key_type
           AND next_label.key_hash = current_label.key_hash
        SET
            current_label.article_id = next_label.article_id,
            current_label.has_existing_article_id =
                next_label.has_existing_article_id;

        TRUNCATE TABLE ojs_stage_next_frontier;
        INSERT INTO ojs_stage_next_frontier
        SELECT
            k.source_record_id,
            COALESCE(
                MIN(
                    CASE WHEN changed_key.has_existing_article_id = 1
                        THEN changed_key.article_id END
                ),
                MIN(changed_key.article_id)
            ) AS next_article_id,
            MAX(changed_key.has_existing_article_id) AS next_has_existing_article_id
        FROM ojs_stage_changed_keys changed_key
        INNER JOIN ojs_stage_keys k
            ON k.key_type = changed_key.key_type
           AND k.key_hash = changed_key.key_hash
        INNER JOIN ojs_stage_assignments current_assignment
            ON current_assignment.source_record_id = k.source_record_id
        GROUP BY k.source_record_id
        HAVING
            next_has_existing_article_id >
                MIN(current_assignment.has_existing_article_id)
            OR (
                next_has_existing_article_id =
                    MIN(current_assignment.has_existing_article_id)
                AND next_article_id < MIN(current_assignment.article_id)
            );

        UPDATE ojs_stage_assignments current_assignment
        INNER JOIN ojs_stage_next_frontier next_assignment
            ON next_assignment.source_record_id =
               current_assignment.source_record_id
        SET
            current_assignment.article_id = next_assignment.article_id,
            current_assignment.has_existing_article_id =
                next_assignment.has_existing_article_id;

        SET changed_rows = ROW_COUNT();
        TRUNCATE TABLE ojs_stage_frontier_sources;
        INSERT INTO ojs_stage_frontier_sources
        SELECT
            source_record_id,
            article_id,
            has_existing_article_id
        FROM ojs_stage_next_frontier;
        SET pass_number = pass_number + 1;
    END WHILE;

    IF changed_rows > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'identity labels did not converge within 64 passes';
    END IF;

    SET @ojs_label_passes = pass_number;
END//
DELIMITER ;
CALL ojs_propagate_labels();
DROP PROCEDURE ojs_propagate_labels;

-- Existing entities bridged by a new/changed source are merged into the oldest
-- stable article ID. Loser IDs remain permanent tombstones with a redirect.
DROP TABLE IF EXISTS ojs_stage_merge_map;
CREATE TABLE ojs_stage_merge_map (
    old_article_id BIGINT UNSIGNED NOT NULL,
    winner_article_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (old_article_id),
    INDEX idx_ojs_stage_merge_winner (winner_article_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_merge_map
SELECT
    candidate.article_id,
    MIN(assignment.article_id)
FROM ojs_stage_existing_candidates candidate
INNER JOIN ojs_stage_assignments assignment
    ON assignment.source_record_id = candidate.source_record_id
WHERE assignment.article_id < candidate.article_id
GROUP BY candidate.article_id;

INSERT INTO ojs_article_events (
    snapshot_date,
    article_id,
    event_type,
    operation,
    version_number,
    redirect_to_article_id,
    previous_data_hash,
    data_hash,
    previous_provenance_hash,
    provenance_hash,
    reason
)
SELECT
    @ojs_snapshot_date,
    loser.article_id,
    'merged',
    'delete',
    loser.version_number + 1,
    merge_map.winner_article_id,
    loser.data_hash,
    loser.data_hash,
    loser.provenance_hash,
    loser.provenance_hash,
    JSON_OBJECT(
        'reason', 'identity_key_bridge',
        'redirect_to_article_id', merge_map.winner_article_id
    )
FROM ojs_stage_merge_map merge_map
INNER JOIN ojs_articles loser
    ON loser.article_id = merge_map.old_article_id
WHERE loser.status <> 'merged'
ORDER BY loser.article_id;

-- Keep old tombstone redirects one hop from the surviving article. A redirect
-- change is itself temporal state, so publish a new merge event and advance the
-- tombstone version before updating its current target below.
INSERT INTO ojs_article_events (
    snapshot_date,
    article_id,
    event_type,
    operation,
    version_number,
    redirect_to_article_id,
    previous_data_hash,
    data_hash,
    previous_provenance_hash,
    provenance_hash,
    reason
)
SELECT
    @ojs_snapshot_date,
    redirect.article_id,
    'merged',
    'delete',
    redirect.version_number + 1,
    merge_map.winner_article_id,
    redirect.data_hash,
    redirect.data_hash,
    redirect.provenance_hash,
    redirect.provenance_hash,
    JSON_OBJECT(
        'reason', 'redirect_flattened',
        'previous_redirect_to_article_id',
        redirect.merged_into_article_id,
        'redirect_to_article_id',
        merge_map.winner_article_id
    )
FROM ojs_articles redirect
INNER JOIN ojs_stage_merge_map merge_map
    ON merge_map.old_article_id = redirect.merged_into_article_id
WHERE redirect.status = 'merged'
ORDER BY redirect.article_id;

UPDATE ojs_articles redirect
INNER JOIN ojs_stage_merge_map merge_map
    ON merge_map.old_article_id = redirect.merged_into_article_id
SET
    redirect.merged_into_article_id = merge_map.winner_article_id,
    redirect.version_number = redirect.version_number + 1,
    redirect.date_modified = @ojs_snapshot_date
WHERE redirect.status = 'merged';

UPDATE ojs_articles loser
INNER JOIN ojs_stage_merge_map merge_map
    ON merge_map.old_article_id = loser.article_id
SET
    loser.status = 'merged',
    loser.merged_into_article_id = merge_map.winner_article_id,
    loser.active_source_count = 0,
    loser.version_number = loser.version_number + 1,
    loser.date_modified = @ojs_snapshot_date,
    loser.date_removed = @ojs_snapshot_date
WHERE loser.status <> 'merged';

UPDATE ojs_article_sources source_alias
INNER JOIN ojs_stage_merge_map merge_map
    ON merge_map.old_article_id = source_alias.article_id
SET
    source_alias.article_id = merge_map.winner_article_id,
    source_alias.date_modified = @ojs_snapshot_date;

UPDATE ojs_article_keys identity_key
INNER JOIN ojs_stage_merge_map merge_map
    ON merge_map.old_article_id = identity_key.article_id
SET identity_key.article_id = merge_map.winner_article_id;

UPDATE ojs_stage_assignments assignment
INNER JOIN ojs_stage_merge_map merge_map
    ON merge_map.old_article_id = assignment.article_id
SET assignment.article_id = merge_map.winner_article_id;

DROP TABLE IF EXISTS ojs_stage_affected_articles;
CREATE TABLE ojs_stage_affected_articles (
    article_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (article_id)
) ENGINE=InnoDB;

INSERT IGNORE INTO ojs_stage_affected_articles
SELECT DISTINCT article_id
FROM ojs_stage_existing_candidates;

INSERT IGNORE INTO ojs_stage_affected_articles
SELECT DISTINCT article_id
FROM ojs_stage_assignments;

INSERT IGNORE INTO ojs_stage_affected_articles
SELECT DISTINCT winner_article_id
FROM ojs_stage_merge_map;

INSERT IGNORE INTO ojs_stage_affected_articles
SELECT DISTINCT source_alias.article_id
FROM ojs_article_sources source_alias
LEFT JOIN ojs_stage_source_index current_source
    ON current_source.source_record_id = source_alias.source_record_id
WHERE source_alias.is_present = 1
  AND current_source.source_record_id IS NULL;

-- Sources absent from the latest full snapshot become inactive aliases. The
-- canonical article is removed only later, after all its aliases are inactive.
UPDATE ojs_article_sources source_alias
LEFT JOIN ojs_stage_source_index current_source
    ON current_source.source_record_id = source_alias.source_record_id
SET
    source_alias.is_present = 0,
    source_alias.is_active = 0,
    source_alias.date_modified = @ojs_snapshot_date,
    source_alias.date_removed = COALESCE(
        source_alias.date_removed,
        @ojs_snapshot_date
    )
WHERE source_alias.is_present = 1
  AND current_source.source_record_id IS NULL;

INSERT INTO ojs_article_sources (
    source_record_id,
    article_id,
    context_id,
    endpoint_id,
    source_identifier,
    application,
    journal_issn,
    journal_title,
    endpoint_oai_url,
    source_oai_identifier,
    record_update_date,
    record_publish_date,
    record_created_at,
    record_modified_at,
    source_removed_at,
    is_present,
    is_active,
    date_added,
    date_modified,
    date_removed,
    title,
    first_creator,
    publication_year,
    doi,
    article_url,
    metadata_hash,
    oai_key_hash,
    doi_key_hash,
    url_key_hash,
    fingerprint_key_hash
)
SELECT
    staged.source_record_id,
    assignment.article_id,
    staged.context_id,
    staged.endpoint_id,
    staged.source_identifier,
    staged.application,
    staged.journal_issn,
    staged.journal_title,
    staged.endpoint_oai_url,
    staged.source_oai_identifier,
    staged.record_update_date,
    staged.record_publish_date,
    staged.record_created_at,
    staged.record_modified_at,
    staged.source_removed_at,
    1,
    staged.is_active,
    @ojs_snapshot_date,
    @ojs_snapshot_date,
    CASE WHEN staged.is_active = 1 THEN NULL ELSE @ojs_snapshot_date END,
    staged.title,
    staged.first_creator,
    staged.publication_year,
    staged.doi,
    staged.article_url,
    staged.metadata_hash,
    staged.oai_key_hash,
    staged.doi_key_hash,
    staged.url_key_hash,
    staged.fingerprint_key_hash
FROM ojs_stage_sources staged
INNER JOIN ojs_stage_assignments assignment
    ON assignment.source_record_id = staged.source_record_id
ON DUPLICATE KEY UPDATE
    date_modified = CASE
        WHEN ojs_article_sources.article_id <> VALUES(article_id)
          OR ojs_article_sources.context_id <> VALUES(context_id)
          OR ojs_article_sources.endpoint_id <> VALUES(endpoint_id)
          OR ojs_article_sources.source_identifier <> VALUES(source_identifier)
          OR NOT (ojs_article_sources.application <=> VALUES(application))
          OR NOT (ojs_article_sources.journal_issn <=> VALUES(journal_issn))
          OR NOT (ojs_article_sources.journal_title <=> VALUES(journal_title))
          OR NOT (ojs_article_sources.endpoint_oai_url <=> VALUES(endpoint_oai_url))
          OR NOT (ojs_article_sources.source_oai_identifier <=> VALUES(source_oai_identifier))
          OR NOT (ojs_article_sources.record_update_date <=> VALUES(record_update_date))
          OR NOT (ojs_article_sources.record_publish_date <=> VALUES(record_publish_date))
          OR NOT (ojs_article_sources.record_created_at <=> VALUES(record_created_at))
          OR NOT (ojs_article_sources.record_modified_at <=> VALUES(record_modified_at))
          OR NOT (ojs_article_sources.source_removed_at <=> VALUES(source_removed_at))
          OR ojs_article_sources.is_present <> 1
          OR ojs_article_sources.is_active <> VALUES(is_active)
          OR NOT (ojs_article_sources.metadata_hash <=> VALUES(metadata_hash))
            THEN @ojs_snapshot_date
        ELSE ojs_article_sources.date_modified
    END,
    date_removed = CASE
        WHEN VALUES(is_active) = 1 THEN NULL
        WHEN ojs_article_sources.is_active = 1
          OR ojs_article_sources.date_removed IS NULL
            THEN @ojs_snapshot_date
        ELSE ojs_article_sources.date_removed
    END,
    article_id = VALUES(article_id),
    context_id = VALUES(context_id),
    endpoint_id = VALUES(endpoint_id),
    source_identifier = VALUES(source_identifier),
    application = VALUES(application),
    journal_issn = VALUES(journal_issn),
    journal_title = VALUES(journal_title),
    endpoint_oai_url = VALUES(endpoint_oai_url),
    source_oai_identifier = VALUES(source_oai_identifier),
    record_update_date = VALUES(record_update_date),
    record_publish_date = VALUES(record_publish_date),
    record_created_at = VALUES(record_created_at),
    record_modified_at = VALUES(record_modified_at),
    source_removed_at = VALUES(source_removed_at),
    is_present = 1,
    is_active = VALUES(is_active),
    title = VALUES(title),
    first_creator = VALUES(first_creator),
    publication_year = VALUES(publication_year),
    doi = VALUES(doi),
    article_url = VALUES(article_url),
    metadata_hash = VALUES(metadata_hash),
    oai_key_hash = VALUES(oai_key_hash),
    doi_key_hash = VALUES(doi_key_hash),
    url_key_hash = VALUES(url_key_hash),
    fingerprint_key_hash = VALUES(fingerprint_key_hash);

-- A first snapshot loads tens of millions of source rows. Build all secondary
-- indexes together afterward instead of maintaining them row by row. Imported
-- clean history already has the complete index set, so incremental runs skip
-- this ALTER and retain indexed source lookups throughout.
DROP PROCEDURE IF EXISTS ojs_ensure_source_indexes;
DELIMITER //
CREATE PROCEDURE ojs_ensure_source_indexes()
BEGIN
    DECLARE source_index_count INT DEFAULT 0;
    DECLARE valid_source_index_count INT DEFAULT 0;
    DECLARE legacy_hash_index_count INT DEFAULT 0;

    SELECT COUNT(DISTINCT index_name)
    INTO source_index_count
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'ojs_article_sources'
      AND index_name IN (
          'idx_ojs_sources_article',
          'idx_ojs_sources_removed',
          'idx_ojs_sources_context_identifier',
          'idx_ojs_sources_oai_key',
          'idx_ojs_sources_doi_key',
          'idx_ojs_sources_url_key',
          'idx_ojs_sources_fingerprint_key'
      );

    SELECT COUNT(*)
    INTO valid_source_index_count
    FROM (
        SELECT
            index_name,
            GROUP_CONCAT(
                column_name
                ORDER BY seq_in_index
                SEPARATOR ','
            ) AS index_columns
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'ojs_article_sources'
          AND index_name IN (
              'idx_ojs_sources_article',
              'idx_ojs_sources_removed',
              'idx_ojs_sources_context_identifier',
              'idx_ojs_sources_oai_key',
              'idx_ojs_sources_doi_key',
              'idx_ojs_sources_url_key',
              'idx_ojs_sources_fingerprint_key'
          )
        GROUP BY index_name
        HAVING
            (
                index_name = 'idx_ojs_sources_article'
                AND index_columns = 'article_id,is_active,source_record_id'
            )
            OR (
                index_name = 'idx_ojs_sources_removed'
                AND index_columns = 'date_removed,source_record_id'
            )
            OR (
                index_name = 'idx_ojs_sources_context_identifier'
                AND index_columns = 'context_id,source_identifier'
            )
            OR (
                index_name = 'idx_ojs_sources_oai_key'
                AND index_columns = 'oai_key_hash,article_id'
            )
            OR (
                index_name = 'idx_ojs_sources_doi_key'
                AND index_columns = 'doi_key_hash,article_id'
            )
            OR (
                index_name = 'idx_ojs_sources_url_key'
                AND index_columns = 'url_key_hash,article_id'
            )
            OR (
                index_name = 'idx_ojs_sources_fingerprint_key'
                AND index_columns = 'fingerprint_key_hash,article_id'
            )
    ) valid_indexes;

    SELECT COUNT(*)
    INTO legacy_hash_index_count
    FROM (
        SELECT
            index_name,
            GROUP_CONCAT(
                column_name
                ORDER BY seq_in_index
                SEPARATOR ','
            ) AS index_columns
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'ojs_article_sources'
          AND index_name IN (
              'idx_ojs_sources_oai_key',
              'idx_ojs_sources_doi_key',
              'idx_ojs_sources_url_key',
              'idx_ojs_sources_fingerprint_key'
          )
        GROUP BY index_name
        HAVING
            (
                index_name = 'idx_ojs_sources_oai_key'
                AND index_columns = 'oai_key_hash'
            )
            OR (
                index_name = 'idx_ojs_sources_doi_key'
                AND index_columns = 'doi_key_hash'
            )
            OR (
                index_name = 'idx_ojs_sources_url_key'
                AND index_columns = 'url_key_hash'
            )
            OR (
                index_name = 'idx_ojs_sources_fingerprint_key'
                AND index_columns = 'fingerprint_key_hash'
            )
    ) legacy_hash_indexes;

    IF source_index_count = 0 THEN
        ALTER TABLE ojs_article_sources
            ADD INDEX idx_ojs_sources_article (
                article_id,
                is_active,
                source_record_id
            ),
            ADD INDEX idx_ojs_sources_removed (
                date_removed,
                source_record_id
            ),
            ADD INDEX idx_ojs_sources_context_identifier (
                context_id,
                source_identifier
            ),
            ADD INDEX idx_ojs_sources_oai_key (
                oai_key_hash,
                article_id
            ),
            ADD INDEX idx_ojs_sources_doi_key (
                doi_key_hash,
                article_id
            ),
            ADD INDEX idx_ojs_sources_url_key (
                url_key_hash,
                article_id
            ),
            ADD INDEX idx_ojs_sources_fingerprint_key (
                fingerprint_key_hash,
                article_id
            );
    ELSEIF
        source_index_count = 7
        AND valid_source_index_count = 3
        AND legacy_hash_index_count = 4
    THEN
        ALTER TABLE ojs_article_sources
            DROP INDEX idx_ojs_sources_oai_key,
            DROP INDEX idx_ojs_sources_doi_key,
            DROP INDEX idx_ojs_sources_url_key,
            DROP INDEX idx_ojs_sources_fingerprint_key,
            ADD INDEX idx_ojs_sources_oai_key (
                oai_key_hash,
                article_id
            ),
            ADD INDEX idx_ojs_sources_doi_key (
                doi_key_hash,
                article_id
            ),
            ADD INDEX idx_ojs_sources_url_key (
                url_key_hash,
                article_id
            ),
            ADD INDEX idx_ojs_sources_fingerprint_key (
                fingerprint_key_hash,
                article_id
            );
    ELSEIF source_index_count <> 7 OR valid_source_index_count <> 7 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'ojs_article_sources has an unexpected secondary index layout';
    END IF;
END//
DELIMITER ;
CALL ojs_ensure_source_indexes();
DROP PROCEDURE ojs_ensure_source_indexes;

-- Bring the effective membership forward to the committed source assignment,
-- then rebuild only touched lookup entries. A corrected key is removed when no
-- retained alias still supplies it. Globally noisy or multi-owner keys are
-- deliberately omitted so they cannot create future accidental merges.
UPDATE ojs_stage_effective_keys effective
INNER JOIN ojs_article_sources source_alias
    ON source_alias.source_record_id = effective.source_record_id
SET
    effective.article_id = source_alias.article_id,
    effective.is_present = source_alias.is_present;

DELETE identity_key
FROM ojs_article_keys identity_key
INNER JOIN ojs_stage_touched_keys touched
    ON touched.key_type = identity_key.key_type
   AND touched.key_hash = identity_key.key_hash;

INSERT INTO ojs_article_keys (
    key_type,
    key_hash,
    key_value,
    article_id,
    date_added
)
SELECT
    effective.key_type,
    effective.key_hash,
    COALESCE(MAX(effective.key_value), MAX(touched.key_value)),
    MIN(effective.article_id),
    COALESCE(MIN(touched.date_added), @ojs_snapshot_date)
FROM ojs_stage_effective_keys effective
INNER JOIN ojs_stage_touched_keys touched
    ON touched.key_type = effective.key_type
   AND touched.key_hash = effective.key_hash
LEFT JOIN ojs_stage_noisy_keys noisy
    ON noisy.key_type = effective.key_type
   AND noisy.key_hash = effective.key_hash
WHERE effective.article_id IS NOT NULL
  AND noisy.key_hash IS NULL
GROUP BY effective.key_type, effective.key_hash
HAVING COUNT(DISTINCT effective.article_id) = 1
   AND COALESCE(MAX(effective.key_value), MAX(touched.key_value)) IS NOT NULL;

-- Rank source aliases only for affected articles. Unchanged canonical payloads
-- are retained byte-for-byte and never reparsed.
DROP TABLE IF EXISTS ojs_stage_article_rollup;
CREATE TABLE ojs_stage_article_rollup (
    article_id BIGINT UNSIGNED NOT NULL,
    source_count BIGINT UNSIGNED NOT NULL,
    active_source_count BIGINT UNSIGNED NOT NULL,
    preferred_source_record_id BIGINT UNSIGNED NOT NULL,
    provenance_hash BINARY(32) NOT NULL,
    PRIMARY KEY (article_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_article_rollup
SELECT
    grouped.article_id,
    grouped.source_count,
    grouped.active_source_count,
    ranked.source_record_id,
    UNHEX(
        SHA2(
            CONCAT_WS(
                ':',
                grouped.source_count,
                grouped.active_source_count,
                grouped.ordered_alias_hashes
            ),
            256
        )
    )
FROM (
    SELECT
        source_alias.article_id,
        COUNT(*) AS source_count,
        SUM(source_alias.is_active) AS active_source_count,
        GROUP_CONCAT(
            SHA2(
                CAST(
                    JSON_ARRAY(
                        source_alias.source_record_id,
                        source_alias.context_id,
                        source_alias.endpoint_id,
                        source_alias.source_identifier,
                        source_alias.application,
                        source_alias.journal_issn,
                        source_alias.journal_title,
                        source_alias.endpoint_oai_url,
                        source_alias.source_oai_identifier,
                        source_alias.record_update_date,
                        source_alias.record_publish_date,
                        source_alias.record_created_at,
                        source_alias.record_modified_at,
                        source_alias.source_removed_at,
                        source_alias.is_present,
                        source_alias.is_active,
                        source_alias.date_added,
                        source_alias.date_modified,
                        source_alias.date_removed,
                        source_alias.title,
                        source_alias.first_creator,
                        source_alias.publication_year,
                        source_alias.doi,
                        source_alias.article_url,
                        HEX(source_alias.metadata_hash),
                        HEX(source_alias.oai_key_hash),
                        HEX(source_alias.doi_key_hash),
                        HEX(source_alias.url_key_hash),
                        HEX(source_alias.fingerprint_key_hash)
                    ) AS CHAR
                ),
                256
            )
            ORDER BY source_alias.source_record_id
            SEPARATOR ':'
        ) AS ordered_alias_hashes
    FROM ojs_stage_affected_articles affected
    INNER JOIN ojs_article_sources source_alias
        ON source_alias.article_id = affected.article_id
    GROUP BY source_alias.article_id
) grouped
INNER JOIN (
    SELECT article_id, source_record_id
    FROM (
        SELECT
            source_alias.article_id,
            source_alias.source_record_id,
            ROW_NUMBER() OVER (
                PARTITION BY source_alias.article_id
                ORDER BY
                    source_alias.is_active DESC,
                    source_alias.is_present DESC,
                    (source_alias.doi IS NOT NULL) DESC,
                    (
                        source_alias.title IS NOT NULL
                        AND source_alias.first_creator IS NOT NULL
                        AND source_alias.publication_year IS NOT NULL
                    ) DESC,
                    CHAR_LENGTH(COALESCE(source_alias.title, '')) DESC,
                    source_alias.source_record_id
            ) AS canonical_rank
        FROM ojs_stage_affected_articles affected
        INNER JOIN ojs_article_sources source_alias
            ON source_alias.article_id = affected.article_id
    ) source_ranks
    WHERE canonical_rank = 1
) ranked
    ON ranked.article_id = grouped.article_id;

DROP TABLE IF EXISTS ojs_stage_payload_needed;
CREATE TABLE ojs_stage_payload_needed (
    article_id BIGINT UNSIGNED NOT NULL,
    source_record_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (article_id),
    INDEX idx_ojs_stage_payload_source (source_record_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_payload_needed
SELECT
    rollup.article_id,
    rollup.preferred_source_record_id
FROM ojs_stage_article_rollup rollup
INNER JOIN ojs_article_sources preferred_source
    ON preferred_source.source_record_id = rollup.preferred_source_record_id
LEFT JOIN ojs_articles existing
    ON existing.article_id = rollup.article_id
WHERE preferred_source.is_present = 1
  AND (
      existing.article_id IS NULL
      OR NOT (
          existing.canonical_source_record_id
          <=>
          rollup.preferred_source_record_id
      )
      OR NOT (
          existing.canonical_metadata_hash
          <=>
          preferred_source.metadata_hash
      )
      OR NOT (existing.application <=> preferred_source.application)
      OR NOT (existing.journal_issn <=> preferred_source.journal_issn)
      OR NOT (existing.journal_title <=> preferred_source.journal_title)
      OR NOT (
          existing.endpoint_oai_url
          <=>
          preferred_source.endpoint_oai_url
      )
      OR NOT (
          existing.source_oai_identifier
          <=>
          preferred_source.source_oai_identifier
      )
      OR NOT (
          existing.record_update_date
          <=>
          preferred_source.record_update_date
      )
      OR NOT (
          existing.record_publish_date
          <=>
          preferred_source.record_publish_date
      )
  );

DROP TABLE IF EXISTS ojs_stage_canonical_payload;
CREATE TABLE ojs_stage_canonical_payload (
    article_id BIGINT UNSIGNED NOT NULL,
    source_record_id BIGINT UNSIGNED NOT NULL,
    application VARCHAR(32) NULL,
    journal_issn CHAR(8) NULL,
    journal_title VARCHAR(512) NULL,
    endpoint_oai_url VARCHAR(2048) NULL,
    source_oai_identifier VARCHAR(1024) NULL,
    record_update_date DATETIME NULL,
    record_publish_date DATETIME NULL,
    title TEXT NULL,
    creators MEDIUMTEXT NULL,
    subjects MEDIUMTEXT NULL,
    description MEDIUMTEXT NULL,
    publisher TEXT NULL,
    published TEXT NULL,
    types TEXT NULL,
    formats TEXT NULL,
    identifiers MEDIUMTEXT NULL,
    source_title TEXT NULL,
    languages TEXT NULL,
    relations MEDIUMTEXT NULL,
    coverage TEXT NULL,
    rights MEDIUMTEXT NULL,
    doi VARCHAR(255) NULL,
    article_url VARCHAR(2048) NULL,
    metadata_xml MEDIUMTEXT NULL,
    metadata_hash BINARY(32) NULL,
    data_hash BINARY(32) NOT NULL,
    PRIMARY KEY (article_id)
) ENGINE=InnoDB;

INSERT INTO ojs_stage_canonical_payload
SELECT
    needed.article_id,
    staged.source_record_id,
    staged.application,
    staged.journal_issn,
    staged.journal_title,
    staged.endpoint_oai_url,
    staged.source_oai_identifier,
    staged.record_update_date,
    staged.record_publish_date,
    staged.title,
    staged.creators,
    staged.subjects,
    staged.description,
    staged.publisher,
    staged.published,
    staged.types,
    staged.formats,
    staged.identifiers,
    staged.source_title,
    staged.languages,
    staged.relations,
    staged.coverage,
    staged.rights,
    staged.doi,
    staged.article_url,
    staged.metadata_xml,
    staged.metadata_hash,
    UNHEX(
        SHA2(
            CONCAT_WS(
                ':',
                HEX(staged.metadata_hash),
                COALESCE(staged.application, ''),
                COALESCE(staged.journal_issn, ''),
                COALESCE(staged.journal_title, ''),
                COALESCE(staged.endpoint_oai_url, ''),
                COALESCE(staged.source_oai_identifier, ''),
                COALESCE(
                    DATE_FORMAT(
                        staged.record_update_date,
                        '%Y-%m-%d %H:%i:%s'
                    ),
                    ''
                ),
                COALESCE(
                    DATE_FORMAT(
                        staged.record_publish_date,
                        '%Y-%m-%d %H:%i:%s'
                    ),
                    ''
                )
            ),
            256
        )
    )
FROM ojs_stage_payload_needed needed
INNER JOIN ojs_stage_sources staged
    ON staged.source_record_id = needed.source_record_id;

-- A preferred alias can be unchanged and therefore absent from the parsed
-- staging table. This only occurs when source removal or a merge changes the
-- canonical choice; parse those exceptional payloads directly from raw.
INSERT INTO ojs_stage_canonical_payload
SELECT
    needed.article_id,
    source_alias.source_record_id,
    source_alias.application,
    source_alias.journal_issn,
    source_alias.journal_title,
    source_alias.endpoint_oai_url,
    NULLIF(ExtractValue(r.metadata, '//header/identifier'), ''),
    source_alias.record_update_date,
    source_alias.record_publish_date,
    NULLIF(ExtractValue(r.metadata, '//dc:title'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:creator'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:subject'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:description'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:publisher'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:date'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:type'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:format'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:identifier'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:source'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:language'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:relation'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:coverage'), ''),
    NULLIF(ExtractValue(r.metadata, '//dc:rights'), ''),
    source_alias.doi,
    source_alias.article_url,
    r.metadata,
    source_alias.metadata_hash,
    UNHEX(
        SHA2(
            CONCAT_WS(
                ':',
                HEX(source_alias.metadata_hash),
                COALESCE(source_alias.application, ''),
                COALESCE(source_alias.journal_issn, ''),
                COALESCE(source_alias.journal_title, ''),
                COALESCE(source_alias.endpoint_oai_url, ''),
                COALESCE(source_alias.source_oai_identifier, ''),
                COALESCE(
                    DATE_FORMAT(
                        source_alias.record_update_date,
                        '%Y-%m-%d %H:%i:%s'
                    ),
                    ''
                ),
                COALESCE(
                    DATE_FORMAT(
                        source_alias.record_publish_date,
                        '%Y-%m-%d %H:%i:%s'
                    ),
                    ''
                )
            ),
            256
        )
    )
FROM ojs_stage_payload_needed needed
INNER JOIN ojs_article_sources source_alias
    ON source_alias.source_record_id = needed.source_record_id
INNER JOIN records r
    ON r.id = needed.source_record_id
LEFT JOIN ojs_stage_sources staged
    ON staged.source_record_id = needed.source_record_id
WHERE staged.source_record_id IS NULL;

DROP TABLE IF EXISTS ojs_stage_article_next;
CREATE TABLE ojs_stage_article_next LIKE ojs_articles;

INSERT INTO ojs_stage_article_next
SELECT
    rollup.article_id,
    CASE
        WHEN rollup.active_source_count > 0 THEN 'active'
        ELSE 'removed'
    END AS status,
    NULL AS merged_into_article_id,
    rollup.preferred_source_record_id,
    rollup.source_count,
    rollup.active_source_count,
    CASE
        WHEN existing.article_id IS NULL THEN 1
        WHEN existing.status <> CASE
                WHEN rollup.active_source_count > 0 THEN 'active'
                ELSE 'removed'
            END
          OR NOT (
              existing.canonical_source_record_id
              <=>
              rollup.preferred_source_record_id
          )
          OR NOT (
              existing.data_hash
              <=>
              COALESCE(payload.data_hash, existing.data_hash)
          )
          OR NOT (
              existing.provenance_hash
              <=>
              rollup.provenance_hash
          )
            THEN existing.version_number + 1
        ELSE existing.version_number
    END AS version_number,
    COALESCE(existing.date_added, @ojs_snapshot_date),
    CASE
        WHEN existing.article_id IS NULL THEN @ojs_snapshot_date
        WHEN existing.status <> CASE
                WHEN rollup.active_source_count > 0 THEN 'active'
                ELSE 'removed'
            END
          OR NOT (
              existing.canonical_source_record_id
              <=>
              rollup.preferred_source_record_id
          )
          OR NOT (
              existing.data_hash
              <=>
              COALESCE(payload.data_hash, existing.data_hash)
          )
          OR NOT (
              existing.provenance_hash
              <=>
              rollup.provenance_hash
          )
            THEN @ojs_snapshot_date
        ELSE existing.date_modified
    END AS date_modified,
    CASE
        WHEN rollup.active_source_count > 0 THEN NULL
        WHEN existing.status = 'removed' THEN existing.date_removed
        ELSE @ojs_snapshot_date
    END AS date_removed,
    CASE WHEN payload.article_id IS NULL THEN existing.application ELSE payload.application END,
    CASE WHEN payload.article_id IS NULL THEN existing.journal_issn ELSE payload.journal_issn END,
    CASE WHEN payload.article_id IS NULL THEN existing.journal_title ELSE payload.journal_title END,
    CASE WHEN payload.article_id IS NULL THEN existing.endpoint_oai_url ELSE payload.endpoint_oai_url END,
    CASE WHEN payload.article_id IS NULL THEN existing.source_oai_identifier ELSE payload.source_oai_identifier END,
    CASE WHEN payload.article_id IS NULL THEN existing.record_update_date ELSE payload.record_update_date END,
    CASE WHEN payload.article_id IS NULL THEN existing.record_publish_date ELSE payload.record_publish_date END,
    CASE WHEN payload.article_id IS NULL THEN existing.title ELSE payload.title END,
    CASE WHEN payload.article_id IS NULL THEN existing.creators ELSE payload.creators END,
    CASE WHEN payload.article_id IS NULL THEN existing.subjects ELSE payload.subjects END,
    CASE WHEN payload.article_id IS NULL THEN existing.description ELSE payload.description END,
    CASE WHEN payload.article_id IS NULL THEN existing.publisher ELSE payload.publisher END,
    CASE WHEN payload.article_id IS NULL THEN existing.published ELSE payload.published END,
    CASE WHEN payload.article_id IS NULL THEN existing.types ELSE payload.types END,
    CASE WHEN payload.article_id IS NULL THEN existing.formats ELSE payload.formats END,
    CASE WHEN payload.article_id IS NULL THEN existing.identifiers ELSE payload.identifiers END,
    CASE WHEN payload.article_id IS NULL THEN existing.source_title ELSE payload.source_title END,
    CASE WHEN payload.article_id IS NULL THEN existing.languages ELSE payload.languages END,
    CASE WHEN payload.article_id IS NULL THEN existing.relations ELSE payload.relations END,
    CASE WHEN payload.article_id IS NULL THEN existing.coverage ELSE payload.coverage END,
    CASE WHEN payload.article_id IS NULL THEN existing.rights ELSE payload.rights END,
    CASE WHEN payload.article_id IS NULL THEN existing.doi ELSE payload.doi END,
    CASE WHEN payload.article_id IS NULL THEN existing.article_url ELSE payload.article_url END,
    CASE WHEN payload.article_id IS NULL THEN existing.metadata_xml ELSE payload.metadata_xml END,
    CASE
        WHEN payload.article_id IS NULL
            THEN existing.canonical_metadata_hash
        ELSE payload.metadata_hash
    END,
    COALESCE(
        payload.data_hash,
        existing.data_hash,
        UNHEX(SHA2(CONCAT('empty:', rollup.article_id), 256))
    ),
    rollup.provenance_hash
FROM ojs_stage_article_rollup rollup
LEFT JOIN ojs_articles existing
    ON existing.article_id = rollup.article_id
LEFT JOIN ojs_stage_canonical_payload payload
    ON payload.article_id = rollup.article_id;

INSERT INTO ojs_article_events (
    snapshot_date,
    article_id,
    event_type,
    operation,
    version_number,
    redirect_to_article_id,
    previous_data_hash,
    data_hash,
    previous_provenance_hash,
    provenance_hash,
    reason
)
SELECT
    @ojs_snapshot_date,
    next_state.article_id,
    CASE
        WHEN existing.article_id IS NULL
         AND next_state.status = 'active'
            THEN 'added'
        WHEN existing.article_id IS NULL
         AND next_state.status = 'removed'
            THEN 'removed'
        WHEN existing.status = 'removed'
         AND next_state.status = 'active'
            THEN 'restored'
        WHEN next_state.status = 'removed'
         AND existing.status <> 'removed'
            THEN 'removed'
        ELSE 'modified'
    END,
    CASE
        WHEN next_state.status = 'active' THEN 'upsert'
        ELSE 'delete'
    END,
    next_state.version_number,
    NULL,
    existing.data_hash,
    next_state.data_hash,
    existing.provenance_hash,
    next_state.provenance_hash,
    JSON_OBJECT(
        'metadata_changed',
        NOT (existing.data_hash <=> next_state.data_hash),
        'provenance_changed',
        NOT (existing.provenance_hash <=> next_state.provenance_hash),
        'status_before',
        existing.status,
        'status_after',
        next_state.status,
        'source_count',
        next_state.source_count,
        'active_source_count',
        next_state.active_source_count
    )
FROM ojs_stage_article_next next_state
LEFT JOIN ojs_articles existing
    ON existing.article_id = next_state.article_id
WHERE existing.article_id IS NULL
   OR existing.status <> next_state.status
   OR NOT (
       existing.canonical_source_record_id
       <=>
       next_state.canonical_source_record_id
   )
   OR NOT (existing.data_hash <=> next_state.data_hash)
   OR NOT (existing.provenance_hash <=> next_state.provenance_hash)
ORDER BY next_state.article_id;

INSERT INTO ojs_articles
SELECT next_state.*
FROM ojs_stage_article_next next_state
LEFT JOIN ojs_articles existing
    ON existing.article_id = next_state.article_id
WHERE existing.article_id IS NULL;

UPDATE ojs_articles existing
INNER JOIN ojs_stage_article_next next_state
    ON next_state.article_id = existing.article_id
SET
    existing.status = next_state.status,
    existing.merged_into_article_id = next_state.merged_into_article_id,
    existing.canonical_source_record_id = next_state.canonical_source_record_id,
    existing.source_count = next_state.source_count,
    existing.active_source_count = next_state.active_source_count,
    existing.version_number = next_state.version_number,
    existing.date_added = next_state.date_added,
    existing.date_modified = next_state.date_modified,
    existing.date_removed = next_state.date_removed,
    existing.application = next_state.application,
    existing.journal_issn = next_state.journal_issn,
    existing.journal_title = next_state.journal_title,
    existing.endpoint_oai_url = next_state.endpoint_oai_url,
    existing.source_oai_identifier = next_state.source_oai_identifier,
    existing.record_update_date = next_state.record_update_date,
    existing.record_publish_date = next_state.record_publish_date,
    existing.title = next_state.title,
    existing.creators = next_state.creators,
    existing.subjects = next_state.subjects,
    existing.description = next_state.description,
    existing.publisher = next_state.publisher,
    existing.published = next_state.published,
    existing.types = next_state.types,
    existing.formats = next_state.formats,
    existing.identifiers = next_state.identifiers,
    existing.source_title = next_state.source_title,
    existing.languages = next_state.languages,
    existing.relations = next_state.relations,
    existing.coverage = next_state.coverage,
    existing.rights = next_state.rights,
    existing.doi = next_state.doi,
    existing.article_url = next_state.article_url,
    existing.metadata_xml = next_state.metadata_xml,
    existing.canonical_metadata_hash = next_state.canonical_metadata_hash,
    existing.data_hash = next_state.data_hash,
    existing.provenance_hash = next_state.provenance_hash
WHERE existing.status <> next_state.status
   OR NOT (
       existing.canonical_source_record_id
       <=>
       next_state.canonical_source_record_id
   )
   OR NOT (existing.data_hash <=> next_state.data_hash)
   OR NOT (existing.provenance_hash <=> next_state.provenance_hash);

INSERT INTO ojs_snapshots (
    snapshot_date,
    dump_completed_at,
    source_filename,
    source_size_bytes,
    source_sha256,
    build_sql_sha256,
    mysql_version,
    full_metadata_rescan,
    source_record_count,
    active_source_count,
    article_count,
    active_article_count,
    removed_article_count,
    merged_article_count,
    event_count
)
SELECT
    @ojs_snapshot_date,
    CAST(@ojs_snapshot_completed_at AS DATETIME),
    @ojs_source_filename,
    @ojs_source_size_bytes,
    @ojs_source_sha256,
    @ojs_build_sql_sha256,
    @ojs_mysql_version,
    @ojs_full_rescan,
    (SELECT COUNT(*) FROM ojs_article_sources),
    (SELECT COUNT(*) FROM ojs_article_sources WHERE is_active = 1),
    (SELECT COUNT(*) FROM ojs_articles),
    (SELECT COUNT(*) FROM ojs_articles WHERE status = 'active'),
    (SELECT COUNT(*) FROM ojs_articles WHERE status = 'removed'),
    (SELECT COUNT(*) FROM ojs_articles WHERE status = 'merged'),
    (
        SELECT COUNT(*)
        FROM ojs_article_events
        WHERE snapshot_date = @ojs_snapshot_date
    );

DROP TABLE IF EXISTS ojs_stage_article_next;
DROP TABLE IF EXISTS ojs_stage_canonical_payload;
DROP TABLE IF EXISTS ojs_stage_payload_needed;
DROP TABLE IF EXISTS ojs_stage_article_rollup;
DROP TABLE IF EXISTS ojs_stage_affected_articles;
DROP TABLE IF EXISTS ojs_stage_merge_map;
DROP TABLE IF EXISTS ojs_stage_next_frontier;
DROP TABLE IF EXISTS ojs_stage_changed_keys;
DROP TABLE IF EXISTS ojs_stage_frontier_sources;
DROP TABLE IF EXISTS ojs_stage_source_labels;
DROP TABLE IF EXISTS ojs_stage_key_labels;
DROP TABLE IF EXISTS ojs_stage_assignments;
DROP TABLE IF EXISTS ojs_stage_existing_candidates;
DROP TABLE IF EXISTS ojs_stage_touched_keys;
DROP TABLE IF EXISTS ojs_stage_noisy_keys;
DROP TABLE IF EXISTS ojs_stage_effective_keys;
DROP TABLE IF EXISTS ojs_stage_keys;
DROP TABLE IF EXISTS ojs_stage_sources;
DROP TABLE IF EXISTS ojs_stage_source_index;
DROP TABLE IF EXISTS ojs_stage_contexts;
