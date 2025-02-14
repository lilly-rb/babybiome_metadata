#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' Loading of the family metadata from the babybiome project into a database
    @Author: 
    @Date: '''

import pandas as pd
import duckdb

def load_connection(path: str):
    return duckdb.connect(path)

def separate_father(df: pd.DataFrame):
    # preparation for transforming into the database

    father = df[['baby', 'time_point', 'probe_date_father', 'probe_date_mpi', 'travel_time_father', 'kit_oral', 'kit_faecal', 'bowels_father', 'probe_abnormalities_notes', 'antibiotics_father', 'antibiotics_father_notes', 'probiotics_father', 'probiotics_notes', 'probiotics_bifido', 'probiotics_e_coli', 'probiotics_lakt']].copy()
    father = father[~((father['antibiotics_father'].isna())&(father['probe_date_father'].isna())&(father['bowels_father'].isna()))] # removes rows with no father samples
    father['sample_id'] = father['baby'] + '-' + 'F' + '-' + father['time_point']
    father['member'] = 'F'

    return father

def separate_mother(df: pd.DataFrame):
    # preparation for transforming into database

    mother = df[['baby', 'time_point', 'probe_date_mother', 'probe_date_mpi', 'travel_time_mother', 'kit_oral', 'kit_faecal', 'bowels_mother', 'probe_abnormalities_notes', 'antibiotics_mother', 'antibiotics_mother_notes', 'probiotics_mother', 'probiotics_notes', 'probiotics_bifido', 'probiotics_e_coli', 'probiotics_lakt', 'weight_mother', 'diabetes_mother', 'diabetes_treatment_opt']].copy()
    mother['sample_id'] = mother['baby'] + '-' + 'M' + '-' + mother['time_point']
    mother['member'] = 'M'

    return mother

def separate_sib1(df: pd.DataFrame):
    # preparation for transforming into database

    sib1 = df[['baby', 'time_point', 'probe_date_sib1', 'probe_date_mpi', 'travel_time_sib1', 'kit_oral', 'kit_faecal', 'bowels_sib1', 'probe_abnormalities_notes', 'antibiotics_sib1', 'antibiotics_sib1_notes', 'probiotics_sib1', 'probiotics_notes', 'probiotics_bifido', 'probiotics_e_coli', 'probiotics_lakt']].copy()
    sib1 = sib1[~((sib1['antibiotics_sib1'].isna())&(sib1['probe_date_sib1'].isna())&(sib1['bowels_sib1'].isna()))]
    sib1['sample_id'] = sib1['baby'] + '-' + 'S' + '-' + sib1['time_point']
    sib1['member'] = 'S'

    return sib1

def separate_sib2(df: pd.DataFrame):
    # preparation for transforming into database

    sib2 = df[['baby', 'time_point', 'probe_date_sib2', 'probe_date_mpi', 'travel_time_sib2', 'kit_oral', 'kit_faecal', 'bowels_sib2', 'probe_abnormalities_notes', 'antibiotics_sib2', 'antibiotics_sib2_notes', 'probiotics_sib2', 'probiotics_notes', 'probiotics_bifido', 'probiotics_e_coli', 'probiotics_lakt']].copy()
    sib2 = sib2[~((sib2['antibiotics_sib2'].isna())&(sib2['probe_date_sib2'].isna())&(sib2['bowels_sib2'].isna()))]
    sib2['sample_id'] = sib2['baby'] + '-' + 'T' + '-' + sib2['time_point']
    sib2['member'] = 'T'

    return sib2

def separate_baby1(df: pd.DataFrame):
    # preparation for transforming into database

    baby1 = df[['baby', 'time_point', 'probe_date_baby1', 'probe_date_mpi', 'travel_time_baby1', 'kit_oral', 'kit_faecal', 'bowels_baby1', 'probe_abnormalities_notes', 'antibiotics_baby1', 'antibiotics_baby1_notes', 'probiotics_baby1', 'probiotics_notes', 'probiotics_bifido', 'probiotics_e_coli', 'probiotics_lakt', 'weight_baby1', 'solids_baby1', 'breastfed_baby1', 'formula_baby1', 'special_diet_baby', 'pacifier_baby1', 'special_diet_baby_notes', 'height_baby1', 'illness_baby1', 'u_untersuchung_abnormalities_notes', 'hospital_baby1_notes', 'feeding_mode']].copy()
    baby1 = baby1[~((baby1['antibiotics_baby1'].isna())&(baby1['probe_date_baby1'].isna())&(baby1['bowels_baby1'].isna()))]
    baby1['sample_id'] = baby1['baby'] + '-' + 'B' + '-' + baby1['time_point']
    baby1['member'] = 'B'

    return baby1

def separate_baby2(df: pd.DataFrame):

    baby2 = df[['baby', 'time_point', 'probe_date_baby2', 'probe_date_mpi', 'travel_time_baby2', 'kit_oral', 'kit_faecal', 'bowels_baby2', 'probe_abnormalities_notes', 'antibiotics_baby2', 'antibiotics_baby2_notes', 'probiotics_baby2', 'probiotics_notes', 'probiotics_bifido', 'probiotics_e_coli', 'probiotics_lakt', 'weight_baby2', 'solids_baby1', 'breastfed_baby1', 'formula_baby1', 'special_diet_baby', 'pacifier_baby2', 'special_diet_baby_notes', 'height_baby2', 'illness_baby2', 'u_untersuchung_abnormalities_notes', 'hospital_baby1_notes', 'feeding_mode']].copy()
    baby2 = baby2[~((baby2['antibiotics_baby2'].isna())&(baby2['probe_date_baby2'].isna())&(baby2['bowels_baby2'].isna()))]
    baby2['sample_id'] = baby2['baby'] + '-' + 'C' + '-' + baby2['time_point']
    baby2['member'] = 'C'

    return baby2

def create_replace_baby_sheet_tables(connection):
    # schemas for the tables etc. can all also be found in corresponding file

    connection.sql('''
                    DROP TABLE IF EXISTS antibiotics;       
                    DROP TABLE IF EXISTS probiotics;
                    DROP TABLE IF EXISTS baby_diet;
                    DROP TABLE IF EXISTS baby_health;
                    DROP TABLE IF EXISTS mother_health;
                    DROP TABLE IF EXISTS collected_samples;
                   ''') # need to drop tables if we want to repopulate from scratch due to dependencies with foreign keys

    connection.sql('''
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
                   ''')
    
    connection.sql('''
                    CREATE TABLE "antibiotics" (
                    "sample_id" varchar REFERENCES collected_samples(sample_id) UNIQUE NOT NULL,
                    "taken" bool,
                    "notes" text
                    );
                    ''')

    connection.sql('''
                    CREATE TABLE "probiotics" (
                    "sample_id" varchar REFERENCES collected_samples(sample_id) UNIQUE NOT NULL,
                    "taken" bool,
                    "bifido" bool,
                    "ecoli" bool,
                    "lakt" bool,
                    "notes" text
                    );
                   ''')
    
    connection.sql('''
                    CREATE TABLE "baby_diet" (
                    "sample_id" varchar REFERENCES collected_samples(sample_id) UNIQUE NOT NULL,
                    "solids" bool,
                    "formula" bool,
                    "breastmilk" bool,
                    "special_diet" varchar,
                    "pacifier" bool,
                    "notes" text
                    );
                   ''')
    
    connection.sql('''
                    CREATE TABLE "baby_health" (
                    "sample_id" varchar REFERENCES collected_samples(sample_id) UNIQUE NOT NULL,
                    "weight" numeric,
                    "height" numeric,
                    "illness" text,
                    "latest_u_results" text,
                    "hospital" text
                    );
                   ''')

    connection.sql('''
                    CREATE TABLE "mother_health" (
                    "sample_id" varchar REFERENCES collected_samples(sample_id) UNIQUE NOT NULL,
                    "weight" numeric,
                    "diabetes" bool,
                    "diabetes_treatment" varchar
                    );
                   ''')

def insert_father(df: pd.DataFrame, connection):

    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point, member, probe_date_father, 
                   probe_date_mpi, travel_time_father, kit_oral, kit_faecal, bowels_father, probe_abnormalities_notes FROM df''')
    
    connection.sql(''' INSERT INTO antibiotics SELECT sample_id, antibiotics_father, antibiotics_father_notes FROM df
                   ''')
    
    connection.sql('''INSERT INTO probiotics SELECT sample_id, probiotics_father, probiotics_bifido, probiotics_e_coli, 
                   probiotics_lakt, probiotics_notes FROM df''')
    
def insert_mother(df: pd.DataFrame, connection):
    
    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point, member, probe_date_mother, 
                   probe_date_mpi, travel_time_mother, kit_oral, kit_faecal, bowels_mother, probe_abnormalities_notes FROM df''')
    
    connection.sql(''' INSERT INTO antibiotics SELECT sample_id, antibiotics_mother, antibiotics_mother_notes FROM df
                   ''')
    
    connection.sql('''INSERT INTO probiotics SELECT sample_id, probiotics_mother, probiotics_bifido, probiotics_e_coli, 
                   probiotics_lakt, probiotics_notes FROM df''')
    
    connection.sql('''INSERT INTO mother_health SELECT sample_id, weight_mother, diabetes_mother, diabetes_treatment_opt FROM df''')

def insert_sib1(df: pd.DataFrame, connection):
    
    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point, member, probe_date_sib1, 
                   probe_date_mpi, travel_time_sib1, kit_oral, kit_faecal, bowels_sib1, probe_abnormalities_notes FROM df''')
    
    connection.sql(''' INSERT INTO antibiotics SELECT sample_id, antibiotics_sib1, antibiotics_sib1_notes FROM df
                   ''')
    
    connection.sql('''INSERT INTO probiotics SELECT sample_id, probiotics_sib1, probiotics_bifido, probiotics_e_coli, 
                   probiotics_lakt, probiotics_notes FROM df''')

def insert_sib2(df: pd.DataFrame, connection):
    
    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point, member, probe_date_sib2, 
                   probe_date_mpi, travel_time_sib2, kit_oral, kit_faecal, bowels_sib2, probe_abnormalities_notes FROM df''')
    
    connection.sql(''' INSERT INTO antibiotics SELECT sample_id, antibiotics_sib2, antibiotics_sib2_notes FROM df
                   ''')
    
    connection.sql('''INSERT INTO probiotics SELECT sample_id, probiotics_sib2, probiotics_bifido, probiotics_e_coli, 
                   probiotics_lakt, probiotics_notes FROM df''')

def insert_baby1(df: pd.DataFrame, connection):
    
    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point, member, probe_date_baby1, 
                   probe_date_mpi, travel_time_baby1, kit_oral, kit_faecal, bowels_baby1, probe_abnormalities_notes FROM df''')
    
    connection.sql(''' INSERT INTO antibiotics SELECT sample_id, antibiotics_baby1, antibiotics_baby1_notes FROM df
                   ''')
    
    connection.sql('''INSERT INTO probiotics SELECT sample_id, probiotics_baby1, probiotics_bifido, probiotics_e_coli, 
                   probiotics_lakt, probiotics_notes FROM df''')
    
    connection.sql('''INSERT INTO baby_diet SELECT sample_id, solids_baby1, formula_baby1, breastfed_baby1, special_diet_baby, 
                   pacifier_baby1, special_diet_baby_notes FROM df''')
    
    connection.sql('''INSERT INTO baby_health SELECT sample_id, weight_baby1, height_baby1, illness_baby1,
                   u_untersuchung_abnormalities_notes, hospital_baby1_notes FROM df''')

def insert_baby2(df: pd.DataFrame, connection):
    
    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point, member, probe_date_baby2, 
                   probe_date_mpi, travel_time_baby2, kit_oral, kit_faecal, bowels_baby2, probe_abnormalities_notes FROM df''')
    
    connection.sql(''' INSERT INTO antibiotics SELECT sample_id, antibiotics_baby2, antibiotics_baby2_notes FROM df
                   ''')
    
    connection.sql('''INSERT INTO probiotics SELECT sample_id, probiotics_baby2, probiotics_bifido, probiotics_e_coli, 
                   probiotics_lakt, probiotics_notes FROM df''')
    
    connection.sql('''INSERT INTO baby_diet SELECT sample_id, solids_baby1, formula_baby1, breastfed_baby1, special_diet_baby, 
                   pacifier_baby2, special_diet_baby_notes FROM df''')
    
    connection.sql('''INSERT INTO baby_health SELECT sample_id, weight_baby2, height_baby2, illness_baby2, 
                   u_untersuchung_abnormalities_notes, hospital_baby1_notes FROM df''')