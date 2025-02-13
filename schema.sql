CREATE TABLE "time_conversions" (
  "categorical" varchar,
  "days" int,
  "months" number,
  "weeks" int
);

CREATE TABLE "member_conversions" (
  "one_letter" char(1),
  "number" int,
  "spelled" varchar,
  "abbreviated" varchar
);

CREATE TABLE "collected_samples" (
  "sample_id" varchar PRIMARY KEY,
  "family" varchar(4),
  "time_point" varchar,
  "member" varchar,
  "sampling_date" date,
  "frozen_date" date,
  "travel_time" int,
  "oral_kit" varchar,
  "faeces_kit" varchar,
  "bowel_movements" varchar,
  "sampling_notes" text
);

CREATE TABLE "antibiotics" (
  "sample_id" varchar,
  "taken" bool,
  "notes" text
);

CREATE TABLE "probiotics" (
  "sample_id" varchar,
  "taken" bool,
  "bifido" bool,
  "ecoli" bool,
  "lakt" bool,
  "species" varchar,
  "notes" text
);

CREATE TABLE "baby_diet" (
  "sample_id" varchar,
  "solids" bool,
  "formula" bool,
  "breastmilk" bool,
  "special_diet" varchar,
  "pacifier" bool,
  "notes" text
);

CREATE TABLE "baby_health" (
  "sample_id" varchar,
  "weight" number,
  "height" number,
  "illness" text,
  "latest_u" text,
  "latest_u_results" text,
  "hospital" text
);

CREATE TABLE "mother_health" (
  "sample_id" varchar,
  "weight" number,
  "diabetes" bool,
  "diabetes_treatment" varchar
);

CREATE TABLE "families" (
  "id" varchar(4) PRIMARY KEY,
  "father" bool,
  "siblings" integer,
  "study_participants" integer
);

CREATE TABLE "diabetes" (
  "id" varchar(4),
  "present" bool,
  "oral_test" bool,
  "treatment" varchar,
  "diagnosis" date,
  "previous_pregnancy" bool
);

CREATE TABLE "mother_details" (
  "id" varchar(4),
  "age" integer,
  "weight_pre_pregnancy" number,
  "height" integer,
  "weight_pre_birth" number,
  "smoke" varchar,
  "alcohol" bool,
  "medicine" varchar,
  "supplements" varchar,
  "antibiotics_past_6" varchar,
  "last_antibiotics" varchar
);

CREATE TABLE "family_health" (
  "id" varchar(4),
  "father_smoke" varchar,
  "sibling_disease" varchar,
  "lactose_int" bool,
  "celiac" bool,
  "antibiotics_past_6" varchar,
  "last_antibiotics" varchar,
  "family_disease" varchar,
  "family_diet" varchar
);

CREATE TABLE "household_details" (
  "id" varchar(4),
  "members" integer,
  "nationality" varchar,
  "pets" varchar
);

CREATE TABLE "birth_details" (
  "id" varchar(4),
  "due_date" date,
  "birth_date" date,
  "sampled" bool,
  "delivery" varchar,
  "location" varchar,
  "water_birth" bool,
  "gender" varchar,
  "birth_weight" number,
  "birth_height" number,
  "abnormalities" varchar,
  "complications" varchar,
  "antibiotics" bool,
  "prebiotics" bool,
  "apgra" varchar,
  "notes" text
);

ALTER TABLE "families" ADD FOREIGN KEY ("id") REFERENCES "collected_samples" ("family");

ALTER TABLE "antibiotics" ADD FOREIGN KEY ("sample_id") REFERENCES "collected_samples" ("sample_id");

ALTER TABLE "probiotics" ADD FOREIGN KEY ("sample_id") REFERENCES "collected_samples" ("sample_id");

ALTER TABLE "baby_diet" ADD FOREIGN KEY ("sample_id") REFERENCES "collected_samples" ("sample_id");

ALTER TABLE "baby_health" ADD FOREIGN KEY ("sample_id") REFERENCES "collected_samples" ("sample_id");

ALTER TABLE "mother_health" ADD FOREIGN KEY ("sample_id") REFERENCES "collected_samples" ("sample_id");

ALTER TABLE "diabetes" ADD FOREIGN KEY ("id") REFERENCES "families" ("id");

ALTER TABLE "mother_details" ADD FOREIGN KEY ("id") REFERENCES "families" ("id");

ALTER TABLE "family_health" ADD FOREIGN KEY ("id") REFERENCES "families" ("id");

ALTER TABLE "household_details" ADD FOREIGN KEY ("id") REFERENCES "families" ("id");

ALTER TABLE "birth_details" ADD FOREIGN KEY ("id") REFERENCES "families" ("id");
