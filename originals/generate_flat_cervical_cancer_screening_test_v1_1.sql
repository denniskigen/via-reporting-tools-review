CREATE DEFINER=`etl_user`@`%` PROCEDURE `generate_flat_cervical_cancer_screening_test_v1_1`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
    SET @primary_table := "flat_cervical_cancer_screening_test";
    SET @query_type := query_type;
    SET @total_rows_written := 0;
    
    SET @encounter_types := "(69,70,147)"; -- 69 Oncology VIA form, 70 Oncology POC Dysplasia form, 147 GynPathology results
    SET @clinical_encounter_types := "(69,70,147)";
    SET @non_clinical_encounter_types := "(-1)";
    SET @other_encounter_types := "(-1)";
                    
    SET @start := now();
    SET @table_version := "flat_cervical_cancer_screening_test_v1.1";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (select max(max_date_created) from etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening_test (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id INT,
        encounter_id INT,
        encounter_type INT,
        encounter_datetime DATETIME,
        visit_id INT,
        location_id INT,
        location_uuid VARCHAR(100),
        uuid VARCHAR(100),
        age INT,
        cur_visit_type_via INT,
        actual_scheduled_date DATETIME,
        gravida SMALLINT,
        parity SMALLINT,
        menstruation_status SMALLINT,
        last_menstrual_period_date DATETIME,
        pregnancy_status SMALLINT,
        estimated_delivery_date DATETIME,
        reason_not_pregnant SMALLINT,
        hiv_status SMALLINT,
        viral_load INT,
        viral_load_date DATETIME,
        prior_via_done SMALLINT,
        prior_via_result SMALLINT,
        prior_via_result_date DATETIME,
        cur_via_result VARCHAR(150),
        visual_impression_cervix VARCHAR(200),
        visual_impression_vagina SMALLINT,
        visual_impression_vulva SMALLINT,
        via_procedure_done VARCHAR(200), -- repeating
        other_via_procedure_done VARCHAR(1000),
        via_management_plan SMALLINT,
        via_management_plan_freetext VARCHAR(1000),
        via_assessment_notes TEXT,
        via_rtc_date DATETIME,
        cur_visit_type_dysplasia SMALLINT,
        prior_dysplasia_history SMALLINT,
        previous_via_result SMALLINT,
        previous_via_result_date DATETIME,
        prior_pap_smear_result SMALLINT,
        prior_biopsy_result SMALLINT,
        prior_biopsy_result_date DATETIME,
        other_biopsy_result_freetext VARCHAR(1000),
        other_biopsy_result_freetext_date DATETIME,
        date_patient_informed_and_referred DATETIME,
        past_treatment_of_dysplasia VARCHAR(150),
        past_treatment_of_dysplasia_freetext VARCHAR(500),
        treatment_specimen_pathology SMALLINT,
        satisfactory_colposcopy SMALLINT,
        colposcopy_findings VARCHAR(150),
        cervical_lesion_size SMALLINT,
        dysplasia_cervix_impression SMALLINT,
        dysplasia_vagina_impression SMALLINT,
        dysplasia_vulva_impression SMALLINT,
        dysplasia_procedure_done SMALLINT,
        dysplasia_procedure_done_freetext VARCHAR(500),
        dysplasia_management_plan VARCHAR(150),
        dysplasia_management_plan_freetext VARCHAR(1000),
        dysplasia_assessment_notes TEXT,
        dysplasia_rtc_date DATETIME,
        gyn_path_pap_smear_results SMALLINT,
        gyn_path_pap_smear_results_date DATETIME,
        biopsy_workup_date DATETIME,
        diagnosis_date DATETIME,
        date_patient_notified_of_biopsy_results DATETIME,
        gyn_path_procedure_done SMALLINT,
        cervix_biopsy_results VARCHAR(200),
        leep_location SMALLINT,
        pathology_vaginal_exam_findings SMALLINT,
        pathology_vulval_exam_findings SMALLINT,
        endometrium_biopsy_exam_findings SMALLINT,
        endocervical_curettage_exam_findings SMALLINT,
        lab_test_freetext VARCHAR(500),
        date_patient_informed_and_referred_gyn_path DATETIME,
        biopsy_results_management_plan VARCHAR(150),
        gyn_path_assessment_notes VARCHAR(1000),
        cancer_staging SMALLINT,
        gyn_path_rtc_date DATETIME,
        prev_encounter_datetime_cervical_cancer_screening DATETIME,
        next_encounter_datetime_cervical_cancer_screening DATETIME,
        prev_encounter_type_cervical_cancer_screening MEDIUMINT,
        next_encounter_type_cervical_cancer_screening MEDIUMINT,
        prev_clinical_datetime_cervical_cancer_screening DATETIME,
        next_clinical_datetime_cervical_cancer_screening DATETIME,
        prev_clinical_location_id_cervical_cancer_screening MEDIUMINT,
        next_clinical_location_id_cervical_cancer_screening MEDIUMINT,
        prev_clinical_rtc_date_cervical_cancer_screening DATETIME,
        next_clinical_rtc_date_cervical_cancer_screening DATETIME,
        PRIMARY KEY encounter_id (encounter_id),
        INDEX person_date (person_id , encounter_datetime),
        INDEX location_enc_date (location_uuid , encounter_datetime),
        INDEX enc_date_location (encounter_datetime , location_uuid),
        INDEX location_id_rtc_date (location_id , via_rtc_date),
        INDEX location_uuid_rtc_date (location_uuid , via_rtc_date),
        INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_cervical_cancer_screening),
        INDEX encounter_type (encounter_type),
        INDEX date_created (date_created)
    );
                        
	IF (@query_type = "build") THEN
		select 'BUILDING..........................................';              												
        SET @write_table := concat("flat_cervical_cancer_screening_test_temp_",queue_number);
        SET @queue_table := concat("flat_cervical_cancer_screening_test_build_queue_", queue_number);                    												
							
        SET @dyn_sql := CONCAT('create table if not exists ', @write_table,' like ', @primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('Create table if not exists ', @queue_table, ' (select * from flat_cervical_cancer_screening_test_build_queue limit ', queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
        
        SET @dyn_sql := CONCAT('delete t1 from flat_cervical_cancer_screening_test_build_queue t1 join ', @queue_table, ' t2 using (person_id);'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
    END IF;
	
    IF (@query_type = "sync") THEN
        select 'SYNCING..........................................';
        SET @write_table := "flat_cervical_cancer_screening_test";
        SET @queue_table := "flat_cervical_cancer_screening_test_sync_queue";

        CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening_test_sync_queue (
            person_id INT PRIMARY KEY
        );                            
                            
        SET @last_update := null;

        SELECT 
            MAX(date_updated)
        INTO @last_update FROM
            etl.flat_log
        WHERE
            table_name = @table_version;										

        replace into flat_cervical_cancer_screening_test_sync_queue
        (select distinct patient_id
          from amrs.encounter
          where date_changed > @last_update
        );

        replace into flat_cervical_cancer_screening_test_sync_queue
        (select distinct person_id
          from etl.flat_obs
          where max_date_created > @last_update
        );

        replace into flat_cervical_cancer_screening_test_sync_queue
        (select distinct person_id
          from etl.flat_lab_obs
          where max_date_created > @last_update
        );

        replace into flat_cervical_cancer_screening_test_sync_queue
        (select distinct person_id
          from etl.flat_orders
          where max_date_created > @last_update
        );
                      
        replace into flat_cervical_cancer_screening_test_sync_queue
        (select person_id from 
          amrs.person 
          where date_voided > @last_update);


        replace into flat_cervical_cancer_screening_test_sync_queue
        (select person_id from 
          amrs.person 
          where date_changed > @last_update);
    END IF;
                      
    -- Remove test patients
    SET @dyn_sql := CONCAT('delete t1 FROM ', @queue_table,' t1
        join amrs.person_attribute t2 using (person_id)
        where t2.person_attribute_type_id=28 and value="true" and voided=0');
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    SET @person_ids_count := 0;
    SET @dyn_sql := CONCAT('select count(*) into @person_ids_count from ', @queue_table); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num patients to update';

    SET @dyn_sql := CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                                    
    SET @total_time := 0;
    SET @cycle_number := 0;
                    
    WHILE @person_ids_count > 0 DO
        SET @loop_start_time = now();
                        
        drop temporary table if exists flat_cervical_cancer_screening_test_build_queue__0;
						
        SET @dyn_sql=CONCAT('create temporary table flat_cervical_cancer_screening_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        drop temporary table if exists flat_cervical_cancer_screening_test_0a;
        SET @dyn_sql = CONCAT(
            'create temporary table flat_cervical_cancer_screening_test_0a
            (select
              t1.person_id,
              t1.visit_id,
              t1.encounter_id,
              t1.encounter_datetime,
              t1.encounter_type,
              t1.location_id,
              t1.obs,
              t1.obs_datetimes,
              case
                  when t1.encounter_type in ', @clinical_encounter_types,' then 1
                  else null
              end as is_clinical_encounter,
              case
                  when t1.encounter_type in ', @non_clinical_encounter_types,' then 20
                  when t1.encounter_type in ', @clinical_encounter_types,' then 10
                  when t1.encounter_type in', @other_encounter_types, ' then 5
                  else 1
              end as encounter_type_sort_index,
              t2.orders
            from etl.flat_obs t1
              join flat_cervical_cancer_screening_test_build_queue__0 t0 using (person_id)
              left join etl.flat_orders t2 using(encounter_id)
            where t1.encounter_type in ',@encounter_types,');'
          );
                            
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
  					
          insert into flat_cervical_cancer_screening_test_0a
          (select
              t1.person_id,
              null,
              t1.encounter_id,
              t1.test_datetime,
              t1.encounter_type,
              null, 
              -- t1.location_id,
              t1.obs,
              null, #obs_datetimes
              -- in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
              0 as is_clinical_encounter,
              1 as encounter_type_sort_index,
              null
              from etl.flat_lab_obs t1
              join flat_cervical_cancer_screening_test_build_queue__0 t0 using (person_id)
          );


          drop temporary table if exists flat_cervical_cancer_screening_test_0;
          create temporary table flat_cervical_cancer_screening_test_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
          (select * from flat_cervical_cancer_screening_test_0a
              order by person_id, date(encounter_datetime), encounter_type_sort_index
          );

          SET @prev_id := null;
          SET @cur_id := null;
          SET @cur_visit_type_via := null;
          SET @actual_scheduled_date := null;
          SET @gravida := null;
          SET @parity := null;
          SET @menstruation_status := null;
          SET @last_menstrual_period_date := null;
          SET @pregnancy_status := null;
          SET @estimated_delivery_date := null;
          SET @reason_not_pregnant := null;
          SET @hiv_status = null;
          SET @viral_load := null;
          SET @viral_load_date := null;
          SET @prior_via_done := null;
          SET @prior_via_result := null;
          SET @prior_via_result_date := null;
          SET @cur_via_result := null;
          SET @visual_impression_cervix := null;
          SET @visual_impression_vagina := null;
          SET @visual_impression_vulva := null;
          SET @via_procedure_done := null;
          SET @other_via_procedure_done := null;
          SET @via_management_plan := null;
          SET @via_management_plan_freetext := null;
          SET @via_assessment_notes := null;
          SET @via_rtc_date := null;
          SET @cur_visit_type_dysplasia := null;
          SET @prior_dysplasia_history := null;
          SET @previous_via_result := null;
          SET @previous_via_result_date := null;
          SET @prior_pap_smear_result := null;
          SET @prior_biopsy_result := null;
          SET @prior_biopsy_result_date := null;
          SET @other_biopsy_result_freetext := null;
          SET @other_biopsy_result_freetext_date := null;
          SET @date_patient_informed_and_referred := null;
          SET @past_treatment_of_dysplasia := null;
          SET @past_treatment_of_dysplasia_freetext := null;
          SET @treatment_specimen_pathology := null;
          SET @satisfactory_colposcopy := null;
          SET @colposcopy_findings := null;
          SET @cervical_lesion_size := null;
          SET @dysplasia_procedure_done := null;
          SET @dysplasia_procedure_done_freetext := null;
          SET @dysplasia_management_plan := null;
          SET @dysplasia_management_plan_freetext := null;
          SET @dysplasia_assessment_notes := null;
          SET @dysplasia_rtc_date := null;
          SET @prior_pap_smear_result := null;
          SET @leep_location = null;
          SET @pathology_vaginal_exam_findings = null;
          SET @pathology_vulval_exam_findings = null;
          SET @endometrium_biopsy_exam_findings = null;
          SET @endocervical_curettage_exam_findings = null;
          SET @lab_test_freetext = null;
          SET @date_patient_informed_and_referred_gyn_path = null;
          SET @biopsy_results_management_plan = null;
          SET @gyn_path_assessment_notes = null;
          SET @cancer_staging = null;
          SET @gyn_path_rtc_date = null;
                                                
          drop temporary table if exists flat_cervical_cancer_screening_test_1;
          create temporary table flat_cervical_cancer_screening_test_1 #(index encounter_id (encounter_id))
          (select 
              obs,
              encounter_type_sort_index,
              @prev_id := @cur_id as prev_id,
              @cur_id := t1.person_id as cur_id,
              t1.person_id,
              t1.encounter_id,
              t1.encounter_type,
              t1.encounter_datetime,
              t1.visit_id,
              -- t4.name as location_name,
              t1.location_id,
              t1.is_clinical_encounter,
              p.gender,
              p.death_date,
              case
                when timestampdiff(year,birthdate,curdate()) > 0 then round(timestampdiff(year,birthdate,curdate()),0)
                else round(timestampdiff(month,birthdate,curdate())/12,2)
              end as age,
              case
                when t1.encounter_type = 69 and obs regexp "!!1839=1911!!" then @cur_visit_type_via := 1 -- New visit
                when t1.encounter_type = 69 and obs regexp "!!1839=1246!!" then @cur_visit_type_via := 2 -- Scheduled visit
                when t1.encounter_type = 69 and obs regexp "!!1839=1837!!" then @cur_visit_type_via := 3 -- Unscheduled visit early
                when t1.encounter_type = 69 and obs regexp "!!1839=1838!!" then @cur_visit_type_via := 4 -- Unscheduled visit late
                else @cur_visit_type_via := null
              end as cur_visit_type_via,
              -- if early/late unscheduled visit, indicate actual scheduled date (  when the patient should have come to clinic)
              case
                when t1.encounter_type = 69 and obs regexp "!!7029=" then @actual_scheduled_date := GetValues(obs, 7029)
                else @actual_scheduled_date := null
              end as actual_scheduled_date,
              case
                when t1.encounter_type = 69 and obs regexp "!!5624=" then @gravida := GetValues(obs, 5624)
                else @gravida := null
              end as gravida,
              case
                when t1.encounter_type = 69 and obs regexp "!!1053=" then @parity := GetValues(obs, 1053)
                else @parity := null
              end as parity,
              case
                when t1.encounter_type = 69 and obs regexp "!!2061=5989!!" then @menstruation_status := 1 -- Menstruating 
                when t1.encounter_type = 69 and obs regexp "!!2061=6496!!" then @menstruation_status := 2 -- Post-menopausal
                else @menstruation_status := null
              end as menstruation_status,
              case
                when t1.encounter_type = 69 and obs regexp "!!1836=" then @last_menstrual_period_date := GetValues(obs, 1836)
                else @last_menstrual_period_date := null
              end as last_menstrual_period_date,
              case
                when t1.encounter_type = 69 and obs regexp "!!8351=1065!!" then @pregnancy_status := 1 -- Yes
                when obs regexp "!!8351=1066!!" then @pregnancy_status := 0 -- No
                else @pregnancy_status := null
              end as pregnancy_status, 
              case
                when t1.encounter_type = 69 and obs regexp "!!5596=" then @estimated_delivery_date := GetValues(obs, 5596)
                else @estimated_delivery_date := null
              end as estimated_delivery_date,
              case
                when t1.encounter_type = 69 and obs regexp "!!9733=9729!!" then @reason_not_pregnant := 1 -- Pregnancy not suspected
                when t1.encounter_type = 69 and obs regexp "!!9733=9730!!" then @reason_not_pregnant := 2 -- Pregnancy test is negative
                when t1.encounter_type = 69 and obs regexp "!!9733=9731!!" then @reason_not_pregnant := 3 -- Hormonal contraception
                when t1.encounter_type = 69 and obs regexp "!!9733=9732!!" then @reason_not_pregnant := 4 -- Postpartum less than six weeks
                else @reason_not_pregnant := null
              end as reason_not_pregnant,
              case
                when t1.encounter_type = 69 and obs regexp "!!6709=664!!" then @hiv_status := 1 -- Negative
                when t1.encounter_type = 69 and obs regexp "!!6709=703!!" then @hiv_status := 2 -- Positive
                when t1.encounter_type = 69 and obs regexp "!!6709=1067!!" then @hiv_status := 3 -- Unknown
                else @hiv_status := null
              end as hiv_status,
              case
                when t1.encounter_type IN (69, 70) and obs regexp "!!856=" then @viral_load := GetValues(obs, 856)
                else @viral_load := null
              end as viral_load,
              case
                when t1.encounter_type IN (69, 70) and obs regexp "!!856=" then @viral_load_date := GetValues(obs_datetimes, 856)
                else @viral_load_date := null
              end as viral_load_date,
              case
                when t1.encounter_type = 69 and obs regexp "!!9589=1065!!" then @prior_via_done := 1 -- Yes
                when t1.encounter_type = 69 and obs regexp "!!9589=1066!!" then @prior_via_done := 0 -- No
                else @prior_via_done := null
              end as prior_via_done,
              case
                when t1.encounter_type = 69 and obs regexp "!!7381=664!!" then @prior_via_result := 1 -- Positive
                when t1.encounter_type = 69 and obs regexp "!!7381=703!!" then @prior_via_result := 2 -- Negative
                else @prior_via_result := null
              end as prior_via_result,
              case
                when t1.encounter_type = 69 and obs regexp "!!9859=1065!!" then @prior_via_result_date := GetValues(obs_datetimes, 7381)
                else @prior_via_result_date := null
              end as prior_via_result_date,
              case
                when t1.encounter_type = 69 and obs regexp "!!9590=" then @cur_via_result := GetValues(obs, 9590)
                else @cur_via_result := null
              end as cur_via_result,
              case
                when t1.encounter_type = 69 and obs regexp "!!7484=" then @visual_impression_cervix := GetValues(obs, 7484)
                else @visual_impression_cervix := null
              end as visual_impression_cervix,
              case
                when t1.encounter_type = 69 and obs regexp "!!7490=1115!!" then @visual_impression_vagina := 1 -- Normal
                when t1.encounter_type = 69 and obs regexp "!!7490=1447!!" then @visual_impression_vagina := 2 -- Warts, genital
                when t1.encounter_type = 69 and obs regexp "!!7490=9181!!" then @visual_impression_vagina := 3 -- Suspicious of cancer, vaginal lesion
                else @visual_impression_vagina := null
              end as visual_impression_vagina,
              case
                when t1.encounter_type = 69 and obs regexp "!!7487=1115!!" then @visual_impression_vulva := 1 -- Normal
                when t1.encounter_type = 69 and obs regexp "!!7487=1447!!" then @visual_impression_vulva := 2 -- Warts, genital
                when t1.encounter_type = 69 and obs regexp "!!7487=9177!!" then @visual_impression_vulva := 3 -- Suspicious of cancer, vulva lesion
                else @visual_impression_vulva := null
              end as visual_impression_vulva,
              case
                when t1.encounter_type = 69 and obs regexp "!!7479=" then @via_procedure_done := GetValues(obs, 7479) -- repeating group
                else @via_procedure_done := null
              end as via_procedure_done,
              case
                when t1.encounter_type = 69 and obs regexp "!!1915=" then @other_via_procedure_done := GetValues(obs, 1915)
                else @other_via_procedure_done := null
              end as other_via_procedure_done,
              case
                when t1.encounter_type = 69 and obs regexp "!!7500=9725!!" then @via_management_plan := 1 -- Return for results
                when t1.encounter_type = 69 and obs regexp "!!7500=9178!!" then @via_management_plan := 2 -- VIA follow-up in 3 to 6 months
                when t1.encounter_type = 69 and obs regexp "!!7500=7496!!" then @via_management_plan := 3 -- Routine yearly VIA
                when t1.encounter_type = 69 and obs regexp "!!7500=7497!!" then @via_management_plan := 4 -- Routine 3 year VIA
                when t1.encounter_type = 69 and obs regexp "!!7500=7383!!" then @via_management_plan := 5 -- Colposcopy planned
                when t1.encounter_type = 69 and obs regexp "!!7500=7499!!" then @via_management_plan := 6 -- Gynecologic oncology clinic referral
                when t1.encounter_type = 69 and obs regexp "!!7500=5622!!" then @via_management_plan := 7 -- Other (non-coded)
                else @via_management_plan := null
              end as via_management_plan, 
              case
                when t1.encounter_type = 69 and obs regexp "!!1915=" then @via_management_plan_freetext := GetValues(obs, 1915)
                else @via_management_plan_freetext := null
              end as via_management_plan_freetext,
              case
                when t1.encounter_type = 69 and obs regexp "!!7222=" then @via_assessment_notes := GetValues(obs, 7222)
                else @via_assessment_notes := null
              end as via_assessment_notes,
              case
                when t1.encounter_type = 69 and obs regexp "!!5096=" then @via_rtc_date := GetValues(obs, 5096)
                else @via_rtc_date := null
              end as via_rtc_date,
              case
                when t1.encounter_type = 70 and obs regexp "!!1839=1911!!" then @cur_visit_type_dysplasia := 1 -- New visit
                when t1.encounter_type = 70 and obs regexp "!!1839=1246!!" then @cur_visit_type_dysplasia := 2 -- Scheduled visit
                when t1.encounter_type = 70 and obs regexp "!!1839=1837!!" then @cur_visit_type_dysplasia := 3 -- Unscheduled visit early
                when t1.encounter_type = 70 and obs regexp "!!1839=1838!!" then @cur_visit_type_dysplasia := 4 -- Unscheduled visit late
                else @cur_visit_type_dysplasia := null
              end as cur_visit_type_dysplasia,
              case
                when t1.encounter_type = 70 and obs regexp "!!7379=1065!!" then @prior_dysplasia_history := 1 -- Yes
                when t1.encounter_type = 70 and obs regexp "!!7379=1066!!" then @prior_dysplasia_history := 0 -- No
                else @prior_dysplasia_history := null
              end as prior_dysplasia_history,
              case
                when t1.encounter_type = 70 and obs regexp "!!7381=664!!" then @previous_via_result := 1 -- Negative
                when t1.encounter_type = 70 and obs regexp "!!7381=703!!" then @previous_via_result := 2 -- Positive
                else @previous_via_result := null
              end as previous_via_result,
              case
                when t1.encounter_type = 70 and obs regexp "!!7381=" then @previous_via_result_date := GetValues(obs_datetimes, 7381)
                else @previous_via_result_date := null
              end as previous_via_result_date,
              case
                when t1.encounter_type = 70 and obs regexp "!!7423=1115!!" then @prior_pap_smear_result := 1 -- Normal
                when t1.encounter_type = 70 and obs regexp "!!7423=7417!!" then @prior_pap_smear_result := 2 -- Atypical squamous cells of undetermined significance
                when t1.encounter_type = 70 and obs regexp "!!7423=7418!!" then @prior_pap_smear_result := 3 -- Atypical glandular cells of undetermined significance
                when t1.encounter_type = 70 and obs regexp "!!7423=7419!!" then @prior_pap_smear_result := 4 -- Low grade squamous intraepithelial lesion
                when t1.encounter_type = 70 and obs regexp "!!7423=7420!!" then @prior_pap_smear_result := 5 -- High grade squamous intraepithelial lesion
                when t1.encounter_type = 70 and obs regexp "!!7423=7421!!" then @prior_pap_smear_result := 6 -- Squamous cell carcinoma, not otherwise specified
                when t1.encounter_type = 70 and obs regexp "!!7423=7422!!" then @prior_pap_smear_result := 7 -- Adenocarcinoma
                else @prior_pap_smear_result := null
              end as prior_pap_smear_result,
              case
                when t1.encounter_type = 70 and obs regexp "!!7426=1115!!" then @prior_biopsy_result := 1
                when t1.encounter_type = 70 and obs regexp "!!7426=7424!!" then @prior_biopsy_result := 2
                when t1.encounter_type = 70 and obs regexp "!!7426=7425!!" then @prior_biopsy_result := 3
                when t1.encounter_type = 70 and obs regexp "!!7426=7216!!" then @prior_biopsy_result := 4
                when t1.encounter_type = 70 and obs regexp "!!7426=7421!!" then @prior_biopsy_result := 5
                else @prior_biopsy_result := null
              end as prior_biopsy_result, 
              case
                when t1.encounter_type = 70 and obs regexp "!!7426=" then @prior_biopsy_result_date := GetValues(obs_datetimes, 7426)
                else @prior_biopsy_result_date := null
              end as prior_biopsy_result_date,
              case
                when t1.encounter_type = 70 and obs regexp "!!7400=" then @other_biopsy_result_freetext := GetValues(obs, 7400)
                else @other_biopsy_result_freetext := null
              end as other_biopsy_result_freetext,
              case
                when t1.encounter_type = 70 and obs regexp "!!7400=" then @other_biopsy_result_freetext_date := GetValues(obs_datetimes, 7400)
                else @other_biopsy_result_freetext_date := null
              end as other_biopsy_result_freetext_date,
              case
                when t1.encounter_type = 70 and obs regexp "!!9706=" then @date_patient_informed_and_referred := GetValues(obs, 9706)
                else @date_patient_informed_and_referred := null
              end as date_patient_informed_and_referred,
              case 
                when t1.encounter_type = 70 and obs regexp "!!7467=" then @past_treatment_of_dysplasia := GetValues(obs, 7467)
                else @past_treatment_of_dysplasia := null
              end as past_treatment_of_dysplasia,
              case
                when t1.encounter_type = 70 and obs regexp "!!1915=" then @past_treatment_of_dysplasia_freetext := GetValues(obs, 1915)
                else @past_treatment_of_dysplasia_freetext := null
              end as past_treatment_of_dysplasia_freetext,
              case
                when t1.encounter_type = 70 and obs regexp "!!7579=1115!!" then @treatment_specimen_pathology := 1
                when t1.encounter_type = 70 and obs regexp "!!7579=149!!" then @treatment_specimen_pathology := 2
                when t1.encounter_type = 70 and obs regexp "!!7579=9620!!" then @treatment_specimen_pathology := 3
                when t1.encounter_type = 70 and obs regexp "!!7579=7424!!" then @treatment_specimen_pathology := 4
                when t1.encounter_type = 70 and obs regexp "!!7579=7425!!" then @treatment_specimen_pathology := 5
                when t1.encounter_type = 70 and obs regexp "!!7579=7216!!" then @treatment_specimen_pathology := 6
                when t1.encounter_type = 70 and obs regexp "!!7579=7421!!" then @treatment_specimen_pathology := 7
                when t1.encounter_type = 70 and obs regexp "!!7579=9618!!" then @treatment_specimen_pathology := 8
                else @treatment_specimen_pathology := null
              end as treatment_specimen_pathology, 
              case
                when t1.encounter_type = 70 and obs regexp "!!7428=1065!!" then @satisfactory_colposcopy := 1
                when t1.encounter_type = 70 and obs regexp "!!7428=1066!!" then @satisfactory_colposcopy := 0
                when t1.encounter_type = 70 and obs regexp "!!7428=1118!!" then @satisfactory_colposcopy := 2
                else @satisfactory_colposcopy := null
              end as satisfactory_colposcopy, 
              case
                when t1.encounter_type = 70 and obs regexp "!!7383=" then @colposcopy_findings := GetValues(obs, 7383)
                else @colposcopy_findings := null
              end as colposcopy_findings,
              case
                when t1.encounter_type = 70 and obs regexp "!!7477=7474!!" then @cervical_lesion_size := 1
                when t1.encounter_type = 70 and obs regexp "!!7477=9619!!" then @cervical_lesion_size := 2
                when t1.encounter_type = 70 and obs regexp "!!7477=7476!!" then @cervical_lesion_size := 3
                else @cervical_lesion_size := null
              end as cervical_lesion_size,
              case
                when t1.encounter_type = 70 and obs regexp "!!7484=1115" then @dysplasia_cervix_impression := 1 -- Normal
                when t1.encounter_type = 70 and obs regexp "!!7484=7424" then @dysplasia_cervix_impression := 2 -- CIN 1
                when t1.encounter_type = 70 and obs regexp "!!7484=7425" then @dysplasia_cervix_impression := 3 -- CIN 2
                when t1.encounter_type = 70 and obs regexp "!!7484=7216" then @dysplasia_cervix_impression := 4 -- CIN 3
                when t1.encounter_type = 70 and obs regexp "!!7484=7421" then @dysplasia_cervix_impression := 5 -- Carcinoma
                else @dysplasia_cervix_impression := null
              end as dysplasia_cervix_impression,
              case
                when t1.encounter_type = 70 and obs regexp "!!7490=1115!!" then @dysplasia_vagina_impression := 1 -- Normal
                when t1.encounter_type = 70 and obs regexp "!!7490=1447!!" then @dysplasia_vagina_impression := 2 -- Warts, genital
                when t1.encounter_type = 70 and obs regexp "!!7490=9181!!" then @dysplasia_vagina_impression := 3 -- Suspicious of cancer, vaginal lesion
                else @dysplasia_vagina_impression := null
              end as dysplasia_vagina_impression,
              case
                when t1.encounter_type = 70 and obs regexp "!!7487=1115!!" then @dysplasia_vulva_impression := 1 -- Normal
                when t1.encounter_type = 70 and obs regexp "!!7487=1447!!" then @dysplasia_vulva_impression := 2 -- Warts, genital
                when t1.encounter_type = 70 and obs regexp "!!7487=9177!!" then @dysplasia_vulva_impression := 3 -- Suspicious of cancer, vulval lesion
                else @dysplasia_vulva_impression := null
              end as dysplasia_vulva_impression,
              case
                when t1.encounter_type = 70 and obs regexp "!!7479=" then @dysplasia_procedure_done := GetValues(obs, 7479)
                else @dysplasia_procedure_done := null
              end as dysplasia_procedure_done,
              case
                when t1.encounter_type = 70 and obs regexp "!!1915=" then @dysplasia_procedure_done_freetext := GetValues(obs, 1915)
                else @dysplasia_procedure_done_freetext := null
              end as dysplasia_procedure_done_freetext,
              case
                when t1.encounter_type = 70 and obs regexp "!!7500=" then @dysplasia_management_plan := GetValues(obs, 7500)
                else @dysplasia_management_plan := null
              end as dysplasia_management_plan,
              case
                when t1.encounter_type = 70 and obs regexp "!!1915=" then @dysplasia_management_plan_freetext := GetValues(obs, 1915)
                else @dysplasia_management_plan_freetext := null
              end as dysplasia_management_plan_freetext,
              case
                when t1.encounter_type = 70 and obs regexp "!!7222=" then @dysplasia_assessment_notes := GetValues(obs, 7222)
                else @dysplasia_assessment_notes := null
              end as dysplasia_assessment_notes,
              case
                when t1.encounter_type = 70 and obs regexp "!!5096=" then @dysplasia_rtc_date := GetValues(obs, 5096)
                else @dysplasia_rtc_date := null
              end as dysplasia_rtc_date,
              case
                when t1.encounter_type = 147 and obs regexp "!!7423=1115" then @gyn_path_pap_smear_results := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!7423=7417" then @gyn_path_pap_smear_results := 2 -- ASCUS
                when t1.encounter_type = 147 and obs regexp "!!7423=7418" then @gyn_path_pap_smear_results := 3 -- AGUS
                when t1.encounter_type = 147 and obs regexp "!!7423=7419" then @gyn_path_pap_smear_results := 4 -- LSIL
                when t1.encounter_type = 147 and obs regexp "!!7423=7421" then @gyn_path_pap_smear_results := 5 -- HSIL
                when t1.encounter_type = 147 and obs regexp "!!7423=7422" then @gyn_path_pap_smear_results := 6 -- Carcinoma
                else @gyn_path_pap_smear_results := null
              end as gyn_path_pap_smear_results,
              case
                when t1.encounter_type = 147 and obs regexp "!!7423=" then @gyn_path_pap_smear_results_date := GetValues(obs_datetimes, 7423)
                else @gyn_path_pap_smear_results_date := null
              end as gyn_path_pap_smear_results_date,
              case
                when t1.encounter_type = 147 and obs regexp "!!10060=" then @biopsy_workup_date := GetValues(obs, 10060)
                else @biopsy_workup_date := null
              end as biopsy_workup_date,
              case
                when t1.encounter_type = 147 and obs regexp "!!9728=" then @diagnosis_date := GetValues(obs, 9278)
                else @diagnosis_date := null
              end as diagnosis_date,
              case
                when t1.encounter_type = 147 and obs regexp "!!10061=" then @date_patient_notified_of_biopsy_results := GetValues(obs, 10061)
                else @date_patient_notified_of_biopsy_results := null
              end as date_patient_notified_of_biopsy_results,
              case 
                when t1.encounter_type = 147 and obs regexp "!!10127=10202!!" then @gyn_path_procedure_done := 1 -- Punch biopsy
                when t1.encounter_type = 147 and obs regexp "!!10127=7147!!" then @gyn_path_procedure_done := 2 -- LEEP
                else @gyn_path_procedure_done := null
              end as gyn_path_procedure_done,
              case
                when t1.encounter_type = 147 and obs regexp "!!7645=" then @cervix_biopsy_results := GetValues(obs, 7645)
                else @cervix_biopsy_results := null
              end as cervix_biopsy_results,
              case
                when t1.encounter_type = 147 and obs regexp "!!8268=8266!!" then @leep_location := 1 -- Superficial 
                when t1.encounter_type = 147 and obs regexp "!!8268=8267!!" then @leep_location := 2 -- Deep
                else @leep_location := null
              end as leep_location,
              case
                when t1.encounter_type = 147 and obs regexp "!!7647=1115!!" then @pathology_vaginal_exam_findings := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!7647=7492!!" then @pathology_vaginal_exam_findings := 2 -- VIN 1
                when t1.encounter_type = 147 and obs regexp "!!7647=7491!!" then @pathology_vaginal_exam_findings := 3 -- VIN 2
                when t1.encounter_type = 147 and obs regexp "!!7647=7435!!" then @pathology_vaginal_exam_findings := 4 -- VIN 3
                when t1.encounter_type = 147 and obs regexp "!!7647=6537!!" then @pathology_vaginal_exam_findings := 5 -- Cervical cancer
                when t1.encounter_type = 147 and obs regexp "!!7647=1447!!" then @pathology_vaginal_exam_findings := 6 -- Warts, genital
                when t1.encounter_type = 147 and obs regexp "!!7647=8282!!" then @pathology_vaginal_exam_findings := 7 -- Cervical squamous metaplasia
                when t1.encounter_type = 147 and obs regexp "!!7647=9620!!" then @pathology_vaginal_exam_findings := 8 -- Condylomata
                when t1.encounter_type = 147 and obs regexp "!!7647=8276!!" then @pathology_vaginal_exam_findings := 9 -- Cervical squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!7647=9617!!" then @pathology_vaginal_exam_findings := 10 -- Microinvasive carcinoma
                when t1.encounter_type = 147 and obs regexp "!!7647=9621!!" then @pathology_vaginal_exam_findings := 11 -- Adenocarcinoma in situ
                when t1.encounter_type = 147 and obs regexp "!!7647=7421!!" then @pathology_vaginal_exam_findings := 12 -- Squamous cell carcinoma, NOS
                when t1.encounter_type = 147 and obs regexp "!!7647=7422!!" then @pathology_vaginal_exam_findings := 13 -- Adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!7647=9618!!" then @pathology_vaginal_exam_findings := 14 -- Invasive adenocarcinoma
                else @pathology_vaginal_exam_findings := null
              end as pathology_vaginal_exam_findings,
              case
                when t1.encounter_type = 147 and obs regexp "!!7646=1115!!" then @pathology_vulval_exam_findings := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!7646=7489!!" then @pathology_vulval_exam_findings := 2 -- Condyloma or vulvar intraepithelial neoplasia grade 1
                when t1.encounter_type = 147 and obs regexp "!!7646=7488!!" then @pathology_vulval_exam_findings := 3 -- Vulvar intraepithelial neoplasia grade 2
                when t1.encounter_type = 147 and obs regexp "!!7646=7483!!" then @pathology_vulval_exam_findings := 4 -- Vulvar intraepithelial neoplasia grade 3
                when t1.encounter_type = 147 and obs regexp "!!7646=9618!!" then @pathology_vulval_exam_findings := 5 -- Invasive adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!7646=1447!!" then @pathology_vulval_exam_findings := 6 -- Warts, genital
                when t1.encounter_type = 147 and obs regexp "!!7646=8282!!" then @pathology_vulval_exam_findings := 7 -- Cervical squamous metaplasia
                when t1.encounter_type = 147 and obs regexp "!!7646=9620!!" then @pathology_vulval_exam_findings := 8 -- Condylomata
                when t1.encounter_type = 147 and obs regexp "!!7646=8276!!" then @pathology_vulval_exam_findings := 9 -- Cervical squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!7646=9617!!" then @pathology_vulval_exam_findings := 10 -- Microinvasive carcinoma
                when t1.encounter_type = 147 and obs regexp "!!7646=9621!!" then @pathology_vulval_exam_findings := 11 -- Adenocarcinoma in situ
                when t1.encounter_type = 147 and obs regexp "!!7646=7421!!" then @pathology_vulval_exam_findings := 12 -- Squamous cell carcinoma, otherwise not specified
                when t1.encounter_type = 147 and obs regexp "!!7646=7422!!" then @pathology_vulval_exam_findings := 13 -- Adenocarcinoma
                else t1.encounter_type = 147 and @pathology_vulval_exam_findings := null
              end as pathology_vulval_exam_findings,
              case
                when t1.encounter_type = 147 and obs regexp "!!10207=1115!!" then @endometrium_biopsy_exam_findings := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!10207=8276!!" then @endometrium_biopsy_exam_findings := 2 -- Cervical squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=9617!!" then @endometrium_biopsy_exam_findings := 3 -- Microinvasive carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=9621!!" then @endometrium_biopsy_exam_findings := 4 -- Adenocarcinoma in situ
                when t1.encounter_type = 147 and obs regexp "!!10207=9618!!" then @endometrium_biopsy_exam_findings := 5 -- Invasive adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=7421!!" then @endometrium_biopsy_exam_findings := 6 -- Squamous cell carcinoma, otherwise not specified
                when t1.encounter_type = 147 and obs regexp "!!10207=8282!!" then @endometrium_biopsy_exam_findings := 7 -- Cervical squamous metaplasia
                when t1.encounter_type = 147 and obs regexp "!!10207=9620!!" then @endometrium_biopsy_exam_findings := 8 -- Condylomata
                when t1.encounter_type = 147 and obs regexp "!!10207=7422!!" then @endometrium_biopsy_exam_findings := 9 -- Adenocarcinoma
                else @endometrium_biopsy_exam_findings := null
              end as endometrium_biopsy_exam_findings,
              case
                when t1.encounter_type = 147 and obs regexp "!!10204=1115!!" then @endocervical_curettage_exam_findings := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!10204=7424!!" then @endocervical_curettage_exam_findings := 2 -- Cervical intraepithelial neoplasia grade 1
                when t1.encounter_type = 147 and obs regexp "!!10204=7425!!" then @endocervical_curettage_exam_findings := 3 -- Cervical intraepithelial neoplasia grade 2
                when t1.encounter_type = 147 and obs regexp "!!10204=7216!!" then @endocervical_curettage_exam_findings := 4 -- Cervical intraepithelial neoplasia grade 3
                when t1.encounter_type = 147 and obs regexp "!!10204=149!!" then  @endocervical_curettage_exam_findings := 5 -- Cervicitis
                when t1.encounter_type = 147 and obs regexp "!!10204=8282!!" then @endocervical_curettage_exam_findings := 6 -- Cervical squamous metaplasia
                when t1.encounter_type = 147 and obs regexp "!!10204=9620!!" then @endocervical_curettage_exam_findings := 7 -- Condylomata
                when t1.encounter_type = 147 and obs regexp "!!10204=8276!!" then @endocervical_curettage_exam_findings := 8 -- Cervical squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10204=9617!!" then @endocervical_curettage_exam_findings := 9 -- Microinvasive carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10204=9621!!" then @endocervical_curettage_exam_findings := 10 -- Adenocarcinoma in situ
                when t1.encounter_type = 147 and obs regexp "!!10204=7421!!" then @endocervical_curettage_exam_findings := 11 -- Squamous cell carcinoma, otherwise not specified
                when t1.encounter_type = 147 and obs regexp "!!10204=7422!!" then @endocervical_curettage_exam_findings := 12 -- Adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!10204=9618!!" then @endocervical_curettage_exam_findings := 13 -- Invasive adenocarcinoma
                else @endocervical_curettage_exam_findings := null
              end as endocervical_curettage_exam_findings,
              case
                when t1.encounter_type = 147 and obs regexp "!!9538=" then @lab_test_freetext := GetValues(obs, 9538)
                else @lab_test_freetext := null
              end as lab_test_freetext,
              case
                when t1.encounter_type = 147 and obs regexp "!!9706=" then @date_patient_informed_and_referred_gyn_path := GetValues(obs, 9706)
                else @date_patient_informed_and_referred_gyn_path := null
              end as date_patient_informed_and_referred_gyn_path,
              case 
                when t1.encounter_type = 147 and obs regexp "!!7500=" then @biopsy_results_management_plan := GetValues(obs, 7500)
                else @biopsy_results_management_plan := null
              end as biopsy_results_management_plan,
              case
                when t1.encounter_type = 147 and obs regexp "!!7222=" then @gyn_path_assessment_notes := GetValues(obs, 7222)
                else @gyn_path_assessment_notes := null
              end as gyn_path_assessment_notes,
              case
                when t1.encounter_type = 147 and obs regexp "!!9868=9852!!" then @cancer_staging := 1 -- Stage I
                when t1.encounter_type = 147 and obs regexp "!!9868=9856!!" then @cancer_staging := 2 -- Stage II
                when t1.encounter_type = 147 and obs regexp "!!9868=9860!!" then @cancer_staging := 3 -- Stage III
                when t1.encounter_type = 147 and obs regexp "!!9868=9864!!" then @cancer_staging := 4 -- Stage IV
                else @cancer_staging := null
              end as cancer_staging,
              case
                when t1.encounter_type = 147 and obs regexp "!!5096=" then @gyn_path_rtc_date := GetValues(obs, 5096)
                else @gyn_path_rtc_date := null
              end as gyn_path_rtc_date
              
						from flat_cervical_cancer_screening_test_0 t1
							join amrs.person p using (person_id)
						order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						SET @prev_id := null;
						SET @cur_id := null;
						SET @prev_encounter_datetime := null;
						SET @cur_encounter_datetime := null;

						SET @prev_clinical_datetime := null;
						SET @cur_clinical_datetime := null;

						SET @next_encounter_type := null;
						SET @cur_encounter_type := null;

            SET @prev_clinical_location_id := null;
						SET @cur_clinical_location_id := null;


						alter table flat_cervical_cancer_screening_test_1 drop prev_id, drop cur_id;

						drop table if exists flat_cervical_cancer_screening_test_2;
						create temporary table flat_cervical_cancer_screening_test_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
                  when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                  else @prev_encounter_datetime := null
							end as next_encounter_datetime_cervical_cancer_screening,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
                  when @prev_id = @cur_id then @next_encounter_type := @cur_encounter_type
                  else @next_encounter_type := null
							end as next_encounter_type_cervical_cancer_screening,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
                  when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                  else @prev_clinical_datetime := null
							end as next_clinical_datetime_cervical_cancer_screening,

              case
                  when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                  else @prev_clinical_location_id := null
							end as next_clinical_location_id_cervical_cancer_screening,

							case
                  when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                  when @prev_id = @cur_id then @cur_clinical_datetime
                  else @cur_clinical_datetime := null
              end as cur_clinic_datetime,

              case
                  when is_clinical_encounter then @cur_clinical_location_id := location_id
                  when @prev_id = @cur_id then @cur_clinical_location_id
                  else @cur_clinical_location_id := null
							end as cur_clinic_location_id,

              case
                  when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                  else @prev_clinical_rtc_date := null
							end as next_clinical_rtc_date_cervical_cancer_screening,

							case
                  when is_clinical_encounter then @cur_clinical_rtc_date := via_rtc_date
                  when @prev_id = @cur_id then @cur_clinical_rtc_date
                  else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_cervical_cancer_screening_test_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_cervical_cancer_screening_test_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

						SET @prev_id := null;
						SET @cur_id := null;
						SET @prev_encounter_type := null;
						SET @cur_encounter_type := null;
						SET @prev_encounter_datetime := null;
						SET @cur_encounter_datetime := null;
						SET @prev_clinical_datetime := null;
						SET @cur_clinical_datetime := null;
            SET @prev_clinical_location_id := null;
						SET @cur_clinical_location_id := null;

						drop temporary table if exists flat_cervical_cancer_screening_test_3;
						create temporary table flat_cervical_cancer_screening_test_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							case
                  when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
                  else @prev_encounter_type:=null
							end as prev_encounter_type_cervical_cancer_screening,	
              @cur_encounter_type := encounter_type as cur_encounter_type,
							case
                  when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                  else @prev_encounter_datetime := null
						  end as prev_encounter_datetime_cervical_cancer_screening,
              @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
							case
                  when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                  else @prev_clinical_datetime := null
							end as prev_clinical_datetime_cervical_cancer_screening,
              case
                  when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                  else @prev_clinical_location_id := null
							end as prev_clinical_location_id_cervical_cancer_screening,
							case
                  when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                  when @prev_id = @cur_id then @cur_clinical_datetime
                  else @cur_clinical_datetime := null
							end as cur_clinical_datetime,
              case
                  when is_clinical_encounter then @cur_clinical_location_id := location_id
                  when @prev_id = @cur_id then @cur_clinical_location_id
                  else @cur_clinical_location_id := null
							end as cur_clinical_location_id,
							case
                  when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                  else @prev_clinical_rtc_date := null
							end as prev_clinical_rtc_date_cervical_cancer_screening,
							case
                  when is_clinical_encounter then @cur_clinical_rtc_date := via_rtc_date
                  when @prev_id = @cur_id then @cur_clinical_rtc_date
                  else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date
							from flat_cervical_cancer_screening_test_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        
					SELECT 
              COUNT(*)
          INTO @new_encounter_rows FROM
              flat_cervical_cancer_screening_test_3;
                    
          SELECT @new_encounter_rows;                    
          SET @total_rows_written = @total_rows_written + @new_encounter_rows;
          SELECT @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
            '(select
              null,
              person_id,
              encounter_id,
              encounter_type,
              encounter_datetime,
              visit_id,
              location_id,
              t2.uuid as location_uuid,
              uuid,
              age,
              cur_visit_type_via,
              actual_scheduled_date,
              gravida,
              parity,
              menstruation_status,
              last_menstrual_period_date,
              pregnancy_status,
              estimated_delivery_date,
              reason_not_pregnant,
              hiv_status,
              viral_load,
              viral_load_date,
              prior_via_done,
              prior_via_result,
              prior_via_result_date,
              cur_via_result,
              visual_impression_cervix,
              visual_impression_vagina,
              visual_impression_vulva,
              via_procedure_done,
              other_via_procedure_done,
              via_management_plan,
              via_management_plan_freetext,
              via_assessment_notes,
              via_rtc_date,
              cur_visit_type_dysplasia,
              prior_dysplasia_history,
              previous_via_result,
              previous_via_result_date,
              prior_pap_smear_result, 
              prior_biopsy_result,
              prior_biopsy_result_date,
              other_biopsy_result_freetext,
              other_biopsy_result_freetext_date,
              date_patient_informed_and_referred,
              past_treatment_of_dysplasia,
              past_treatment_of_dysplasia_freetext,
              treatment_specimen_pathology,
              satisfactory_colposcopy,
              colposcopy_findings,
              cervical_lesion_size,
              dysplasia_cervix_impression,
              dysplasia_vagina_impression,
              dysplasia_vulva_impression,
              dysplasia_procedure_done,
              dysplasia_procedure_done_freetext,
              dysplasia_management_plan,
              dysplasia_management_plan_freetext,
              dysplasia_assessment_notes,
              dysplasia_rtc_date,
              gyn_path_pap_smear_results,
              gyn_path_pap_smear_results_date,
              biopsy_workup_date,
              diagnosis_date,
              date_patient_notified_of_biopsy_results,
              gyn_path_procedure_done,
              cervix_biopsy_results,
              leep_location,
              pathology_vaginal_exam_findings,
              pathology_vulval_exam_findings,
              endometrium_biopsy_exam_findings,
              endocervical_curettage_exam_findings,
              lab_test_freetext,
              date_patient_informed_and_referred_gyn_path,
              biopsy_results_management_plan,
              gyn_path_assessment_notes,
              cancer_staging,
              gyn_path_rtc_date,
              prev_encounter_datetime_cervical_cancer_screening,
              next_encounter_datetime_cervical_cancer_screening,
              prev_encounter_type_cervical_cancer_screening,
              next_encounter_type_cervical_cancer_screening,
              prev_clinical_datetime_cervical_cancer_screening,
              next_clinical_datetime_cervical_cancer_screening,
              prev_clinical_location_id_cervical_cancer_screening,
              next_clinical_location_id_cervical_cancer_screening,
              prev_clinical_rtc_date_cervical_cancer_screening,
              next_clinical_rtc_date_cervical_cancer_screening

              from flat_cervical_cancer_screening_test_3 t1
              join amrs.location t2 using (location_id))');       

					PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_cervical_cancer_screening_test_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                              
					SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;

					SET @cycle_length := timestampdiff(second,@loop_start_time,now());
          SET @total_time := @total_time + @cycle_length;
          SET @cycle_number := @cycle_number + 1;
          
          SET @remaining_time := ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
      SELECT 
          @person_ids_count AS 'persons remaining',
          @cycle_length AS 'Cycle time (s)',
          CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
          @remaining_time AS 'Est time remaining (min)';

    END WHILE;
                 
    IF (@query_type = "build") THEN
        SET @dyn_sql := CONCAT('drop table ', @queue_table, ';'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                        
        SET @total_rows_to_write := 0;
        SET @dyn_sql := CONCAT("Select count(*) into @total_rows_to_write from ", @write_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
                                                
        SET @start_write := now();
        SELECT 
            CONCAT(@start_write,
                ' : Writing ',
                    @total_rows_to_write,
                ' to ',
                    @primary_table);

						SET @dyn_sql := CONCAT('replace into ', @primary_table, '(select * from ',@write_table,');');
            PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;
						
            SET @finish_write := now();
            SET @time_to_write := timestampdiff(second,@start_write,@finish_write);

            SELECT 
                CONCAT(@finish_write,
                        ' : Completed writing rows. Time to write to primary table: ',
                        @time_to_write,
                        ' seconds ');                        
                        
            SET @dyn_sql := CONCAT('drop table ',@write_table,';'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
    END IF;
                
		SET @ave_cycle_length := ceil(@total_time/@cycle_number);

    SELECT 
        CONCAT('Average Cycle Length: ',
                @ave_cycle_length,
                ' second(s)');
                
        SET @end := now();
        insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				SELECT 
            CONCAT(@table_version,
                    ' : Time to complete: ',
                    TIMESTAMPDIFF(MINUTE, @start, @end),
                    ' minutes');
END