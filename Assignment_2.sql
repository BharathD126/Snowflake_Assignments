-- Task 1: Building a Pipeline with Streams and Tasks

CREATE OR REPLACE TABLE  PATIENT_T (   
    PATIENT_ID NUMBER NOT NULL,
    PATIENT_NAME VARCHAR2(40),
    AGE NUMBER,
    GENDER VARCHAR2(12),
    LOCATION VARCHAR2(40),
    PHONE_NUMBER NUMBER,
    DOCTOR_ID NUMBER,
    REGISTRATION_DATE DATE,
    ADD_TIME DATETIME,
    CHANGE_TIME DATETIME,
    ROW_IND VARCHAR(1)
    );

 
INSERT INTO PATIENT_T (PATIENT_ID, PATIENT_NAME, AGE, GENDER,LOCATION,PHONE_NUMBER,DOCTOR_ID, REGISTRATION_DATE,ADD_TIME,CHANGE_TIME,ROW_IND) VALUES
(1,'Bharath',25,'M','HYD',99909999777,12,'12-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(2,'Santhosh',30,'M','HYD',99909999444,12,'23-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(3,'Swathi',30,'F','HYD',99909999555,7,'25-OCT-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(4,'Raj',30,'M','HYD',99909999666,12,'01-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(5,'Vimala',25,'F','HYD',99909999089,12,'25-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(6,'Varun',30,'M','BLR',99909999111,10,'27-AUG-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(7,'Ved',20,'M','BLR',99909999222,12,'10-SEP-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(8,'Sai',10,'M','HYD',99909999333,7,'20-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(9,'Krishna',30,'M','BLR',99909999000,7,'27-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(10,'Vinay', 35,'M','HYD',99909999898,12,'27-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I');

select * from patient_t;

create or replace stream Staging_PatientStream on table patient_t;


INSERT INTO PATIENT_T (PATIENT_ID, PATIENT_NAME, AGE, GENDER,LOCATION,PHONE_NUMBER,DOCTOR_ID, REGISTRATION_DATE,ADD_TIME,CHANGE_TIME,ROW_IND) VALUES
(11,'Ram',38,'F','BRL',99909999767,10,'27-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'),
(12,'Vamsi',25,'M','HYD',99909999089,12,'25-NOV-2024',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'I'); 

UPDATE PATIENT_T SET LOCATION = 'Chennai',CHANGE_TIME = CURRENT_TIMESTAMP,ROW_IND = 'I' WHERE PATIENT_ID = 8;
 
select * from Staging_PatientStream ;
 
create or replace table Final_PatientRecords (Location varchar, REGISTRATION_DATE DATE,PatientCount Number);

 
 
create or replace task Load_Final_PatientRecords warehouse = 'COMPUTE_WH' schedule = '1 minute' 
when SYSTEM$STREAM_HAS_DATA('Staging_patientStream') AS insert into Final_PatientRecords select Location,REGISTRATION_DATE, count(*) as patientCount from Staging_patientStream 
WHERE METADATA$ACTION = 'INSERT'
group by Location,REGISTRATION_DATE;
 
 
alter task Load_Final_PatientRecords resume;
 
----- suspend after 5 min-------
alter task Load_Final_PatientRecords suspend;
 

select * from Staging_PatientStream ; --Stream is empty after task execution
 
select * from Final_PatientRecords; -- Data loaded into Final_PatientRecords


-------------------------------------------------------------------------------------------------------------------------------------------

-- Task-2: Query Optimization with Clustering Keys

--Example -1:
CREATE TABLE LINEITEM AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM;

--Before creating clustering key 
SELECT L_SHIPMODE, count(*) from LINEITEM where L_SHIPDATE < '2024-10-01' group by L_SHIPMODE; --total query time (compile + execution ) -- 1s


alter table LINEITEM cluster by (L_SHIPDATE); -- adding clustering key

--After creating clustering key 
SELECT L_SHIPMODE, count(*) from LINEITEM where L_SHIPDATE < '2024-10-01' group by L_SHIPMODE; -- total query time (compile + execution ) - 104ms


-- Example-2:
CREATE TABLE ORDERS AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

SELECT O_ORDERSTATUS, count(*) from ORDERS where O_ORDERDATE < '2024-10-01' group by O_ORDERSTATUS;   -- total query time (compile + execution ) -- 134ms
 
alter table ORDERS cluster by (O_ORDERDATE); -- adding clustering key
 
SELECT O_ORDERSTATUS, count(*) from ORDERS where O_ORDERDATE < '2024-10-01' group by O_ORDERSTATUS;  -- total query time (compile + execution ) - 23ms



