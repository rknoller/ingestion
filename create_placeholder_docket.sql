-- Create placeholder docket with id=2147483647 for auto-generated opinion clusters
-- This docket serves as a fallback FK target when creating placeholder clusters

INSERT INTO search_docket (
    id, 
    date_created, 
    date_modified, 
    case_name_short, 
    case_name, 
    case_name_full, 
    slug, 
    docket_number, 
    blocked, 
    court_id, 
    cause, 
    filepath_ia, 
    filepath_local, 
    jurisdiction_type, 
    jury_demand, 
    nature_of_suit, 
    pacer_case_id, 
    source, 
    assigned_to_str, 
    referred_to_str, 
    view_count, 
    appeal_from_str, 
    appellate_case_type_information, 
    appellate_fee_status, 
    panel_str, 
    mdl_status, 
    filepath_ia_json, 
    docket_number_core, 
    federal_dn_case_type, 
    federal_dn_judge_initials_assigned, 
    federal_dn_judge_initials_referred, 
    federal_dn_office_code
) VALUES (
    2147483647,           -- id (INT_MAX)
    NOW(),                -- date_created
    NOW(),                -- date_modified
    'PLACEHOLDER',        -- case_name_short
    'PLACEHOLDER',        -- case_name
    'PLACEHOLDER',        -- case_name_full
    'placeholder-max',    -- slug (varchar(75), must be unique)
    '',                   -- docket_number
    false,                -- blocked
    'scotus',             -- court_id (varchar(15), NOT NULL - using Supreme Court)
    '',                   -- cause
    '',                   -- filepath_ia
    '',                   -- filepath_local
    '',                   -- jurisdiction_type
    '',                   -- jury_demand
    '',                   -- nature_of_suit
    NULL,                 -- pacer_case_id
    0,                    -- source (smallint, NOT NULL - 0 for default/unknown)
    '',                   -- assigned_to_str
    '',                   -- referred_to_str
    0,                    -- view_count
    '',                   -- appeal_from_str
    '',                   -- appellate_case_type_information
    '',                   -- appellate_fee_status
    '',                   -- panel_str
    '',                   -- mdl_status
    '',                   -- filepath_ia_json
    '',                   -- docket_number_core
    '',                   -- federal_dn_case_type
    '',                   -- federal_dn_judge_initials_assigned
    '',                   -- federal_dn_judge_initials_referred
    ''                    -- federal_dn_office_code
)
ON CONFLICT (id) DO NOTHING;
