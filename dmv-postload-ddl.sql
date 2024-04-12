drop index if exists person_doc_id;
drop index if exists person_gov_id;
drop index if exists person_audit_log_doc_id_version;
drop index if exists person_audit_log_gov_id;

drop index if exists vehicle_doc_id;
drop index if exists vehicle_audit_log_doc_id_version;
drop index if exists vehicle_audit_log_vin;

drop index if exists vehicle_registration_doc_id;
drop index if exists vehicle_registration_license_plate_num;
drop index if exists vehicle_registration_audit_log_doc_id_version;
drop index if exists vehicle_registration_audit_log_license_plate_num;
drop index if exists vehicle_registration_audit_log_vin;

drop index if exists drivers_license_license_num;
drop index if exists drivers_license_audit_log_doc_id_version;
drop index if exists drivers_license_audit_log_person_id;
drop index if exists drivers_license_audit_log_license_num;

alter table dmv.person add primary key (person_id);
create unique index person_doc_id on dmv.person (doc_id);
create index person_gov_id on dmv.person (gov_id);

create unique index person_audit_log_doc_id_version on dmv.person_audit_log (doc_id, version);
create index person_audit_log_gov_id on dmv.person_audit_log (gov_id);

alter table dmv.vehicle add primary key (vin);
create unique index vehicle_doc_id on dmv.vehicle (doc_id);

create unique index vehicle_audit_log_doc_id_version on dmv.vehicle_audit_log (doc_id, version);
create index vehicle_audit_log_vin on dmv.vehicle_audit_log (vin);

alter table dmv.vehicle_registration add primary key (vin);
create unique index vehicle_registration_doc_id on dmv.vehicle_registration (doc_id);
create index vehicle_registration_license_plate_num on dmv.vehicle_registration (license_plate_num);

create unique index vehicle_registration_audit_log_doc_id_version on dmv.vehicle_registration_audit_log (doc_id, version);
create index vehicle_registration_audit_log_license_plate_num on dmv.vehicle_registration_audit_log (license_plate_num);
create index vehicle_registration_audit_log_vin on dmv.vehicle_registration_audit_log (vin);

alter table dmv.drivers_license add primary key (person_id);
create index drivers_license_license_plate_num on dmv.drivers_license (license_plate_num);

create unique index drivers_license_audit_log_doc_id_version on dmv.drivers_license_audit_log (doc_id, version);
create index drivers_license_audit_log_person_id on dmv.drivers_license_audit_log (person_id);
create index drivers_license_audit_log_license_plate_num on dmv.drivers_license_audit_log (license_plate_num);