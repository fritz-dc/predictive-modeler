SELECT
    school.schoolid  AS "school_id",
    teacher.personid  AS "teacher_id",
    project.project_id  AS "project_id",
        (DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', project_workflow_facts.last_content_or_resource_approved_at ))) AS "project_last_posted_date",
        (DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', project_workflow_facts.last_funded_at ))) AS "project_funded_date",
        (DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', project.expired_at ))) AS "project_expiration_date"
FROM dbt_target.fct_projects  AS project
LEFT JOIN dbt_target.fct_project_workflows  AS project_workflow_facts ON project.project_id = project_workflow_facts.project_id
LEFT JOIN dcteacher  AS teacher_ ON teacher_.teacherid = project.teacher_id
LEFT JOIN ${users.SQL_TABLE_NAME} AS teacher ON teacher_.teacherid = teacher.personid
LEFT JOIN school  AS school ON school.schoolid = project.school_id
LEFT JOIN address  AS school_address ON school.addressid = school_address.addressid
WHERE (NOT (project.is_essentials_list ) OR (project.is_essentials_list ) IS NULL) AND (NOT ((UPPER(school_address.state)) = 'PR' ) OR ((UPPER(school_address.state)) = 'PR' ) IS NULL) AND ((project.project_id ) > 0 OR (project.project_id ) IS NULL) AND project_workflow_facts.last_content_or_resource_approved_at >= '2021-07-01'
