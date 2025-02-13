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

def create_replace_baby_sheet_tables(connection):
    # schemas for the tables etc. can all also be found in corresponding file

    connection.sql('''
                    DROP TABLE IF EXISTS antibiotics;       
                    DROP TABLE IF EXISTS probiotics;
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
                    "species" varchar,
                    "notes" text
                    );
                   ''')
    
def insert_father(df: pd.DataFrame, connection):

    connection.sql('''INSERT INTO collected_samples SELECT sample_id, baby, time_point,member, probe_date_father, 
                   probe_date_mpi, travel_time_father, kit_oral, kit_faecal, bowels_father, probe_abnormalities_notes FROM df''')

