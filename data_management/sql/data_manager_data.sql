-- kpmp_dvc_integration.data_manager_data definition

CREATE TABLE `data_manager_data` (
                                     `id` int(11) DEFAULT 0,
                                     `dlu_package_id` varchar(100) DEFAULT NULL,
                                     `dlu_created` datetime NOT NULL,
                                     `dlu_submitter` varchar(100) DEFAULT NULL,
                                     `dlu_tis` varchar(100) DEFAULT NULL,
                                     `dlu_packageType` varchar(100) DEFAULT NULL,
                                     `dlu_subject_id` varchar(200) DEFAULT NULL,
                                     `dlu_error` tinyint(1) DEFAULT 0,
                                     `redcap_id` text DEFAULT NULL,
                                     `known_specimen` text DEFAULT NULL,
                                     `user_package_ready` char(1) DEFAULT 'N',
                                     `package_validated` text DEFAULT NULL,
                                     `ready_to_move_from_globus` varchar(100) DEFAULT NULL,
                                     `globus_dlu_status` varchar(255) DEFAULT NULL,
                                     `package_status` text DEFAULT NULL,
                                     `current_owner` varchar(100) DEFAULT NULL,
                                     `ar_promotion_status` varchar(100) DEFAULT NULL,
                                     `sv_promotion_status` varchar(100) DEFAULT NULL,
                                     `release_version` varchar(100) DEFAULT NULL,
                                     `removed_from_globus` varchar(100) DEFAULT NULL,
                                     `notes` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;