CREATE TABLE biopsy_tracking_long
(
    redcap_id       varchar(100) NULL,
    specimen_id     varchar(100) NULL,
    dlu_tis         varchar(100) NULL,
    dlu_packageType varchar(100) NULL,
    status          varchar(100) NULL
) ENGINE=InnoDB
DEFAULT CHARSET=latin1
COLLATE=latin1_swedish_ci;