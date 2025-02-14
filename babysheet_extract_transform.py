#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' Extraction and transformation of the family metadata baby sheets (time point specific) from the babybiome project
    @Author: LRB
    @Date: 12.02.2025'''

import pandas as pd
from datetime import date
import openpyxl

def load_baby_sheet(path: str, sheet: str):
    return pd.read_excel(path, sheet_name = sheet, header = None)

def load_baby_sheet_notes(path: str, sheet: str):
    workbook = openpyxl.load_workbook(path)
    worksheet = workbook[sheet]
    normal = pd.DataFrame(worksheet.values) # for column labels
    
    for row in worksheet.iter_rows():
        for cell in row:
            cell_comment = cell.comment
            if cell_comment:
                cell.value = cell_comment.text
            else:
                cell.value = None
    
    notes = pd.DataFrame(worksheet.values)
    notes.iloc[:, 1] = normal.iloc[:, 1] + "_notes" # adds column labels back

    return notes

def merge_normal_notes(df1: pd.DataFrame, df2: pd.DataFrame):
    return pd.concat([df1, df2], axis = 0)

def rough_clean(df: pd.DataFrame):
    df.drop(df.columns[0], axis = 1, inplace=True) # rids english labs
    df.dropna(thresh=2, axis = 1, inplace = True) # this drops all empty time points (they have the Frgaebogen field and nothing else)
    df.dropna(how = 'all', axis = 0, inplace = True)
    return df.T  

def add_info_cols(df: pd.DataFrame, sheet: str):

    # baby col
    baby = pd.Series(["baby"] + [sheet] * (df.shape[0]-1))
    # timepoint col (categorical)
    df.iloc[:, 0] = df.iloc[:, 0].str.replace('Fragebogen ', '')
    df.iloc[:, 0] = df.iloc[:, 0].str.replace('"', '')
    df.iloc[:, 0] = df.iloc[:, 0].str.replace(' ', '')
    df.iloc[:, 0] = df.iloc[:, 0].str.split('Ver', n = 1).str[0]
    df.iloc[:, 0] = df.iloc[:, 0].str.split('wied', n = 1).str[0]
    df.iloc[:, 0] = df.iloc[:, 0].str.split('Gebu', n = 1).str[0]

    df.iloc[0, 0] = 'time_point'

    df.reset_index(inplace = True, drop = True)

    return pd.concat([df, baby], axis = 1, ignore_index = True)

def set_col_names(df: pd.DataFrame, path: str):

    df.columns = df.iloc[0]
    df = df.iloc[1:, :].copy()
    df.reset_index(drop = True, inplace = True)

    new_names_notes = {}
    new_names = dict(pd.read_excel(path).values) # has to be excel bc of sonderzeichen
    for key, value in new_names.items(): new_names_notes[key + "_notes"] = value + "_notes"
    df.rename(new_names, axis = 1, inplace = True, errors = 'ignore')
    df.rename(new_names_notes, axis = 1, inplace = True, errors = 'ignore')

    return df

def deleting_cols(df: pd.DataFrame, path: str):

    delete_names = pd.read_excel(path)["Old"].to_list() # has to be excel bc of sonderzeichen
    delete_names_notes = [name + "_notes" for name in delete_names]
    df.drop(delete_names, axis = 1, inplace = True, errors = 'ignore')
    df.drop(delete_names_notes, axis = 1, inplace = True, errors = 'ignore')

    return df

def prepare_baby_sheet(sheet_path: str, sheet: str, renaming_path: str, deleting_path: str):
    
    df = rough_clean(merge_normal_notes(load_baby_sheet(sheet_path, sheet), load_baby_sheet_notes(sheet_path, sheet)))
    df = add_info_cols(df, sheet)
    df = set_col_names(df, renaming_path)
    df = deleting_cols(df, deleting_path)

    return df

def merge_babies(df_list: list):
    return pd.concat(df_list, axis = 0, ignore_index = True)

def run_through_babies(first: int, last: int, skips: list, path: str, renaming_path: str, deleting_path: str):

    babies_list = []

    for i in range(first, last + 1):
        if i in skips:
            continue
        sheet = f'B{i:03}'
        df = prepare_baby_sheet(path, sheet, renaming_path, deleting_path)
        babies_list.append(df)
    
    return merge_babies(babies_list)

def edit_probiotics(df: pd.DataFrame):

    probiotics_m = pd.Series(df['probiotics_notes'].str.contains(r'mutter', case = False), name = 'probiotics_mother')
    probiotics_s = pd.Series(df['probiotics_notes'].str.contains(r'kind', case = False), name = 'probiotics_sib1')
    probiotics_t = pd.Series(df['probiotics_notes'].str.contains(r'child', case = False), name = 'probiotics_sib2')
    probiotics_f = pd.Series(df['probiotics_notes'].str.contains(r'vater', case = False), name = 'probiotics_father')
    probiotics_b = pd.Series(df['probiotics_notes'].str.contains(r'baby', case = False), name = 'probiotics_baby1')
    probiotics_c = pd.Series(df['probiotics_notes'].str.contains(r'infant', case = False), name = 'probiotics_baby2')

    probiotics_bifido = pd.Series(df["probiotics_notes"].str.contains(r"[\S\s]*bifido[\S\s]*", case = False, regex = True), name = "probiotics_bifido")
    probiotics_lakt = pd.Series(df["probiotics_notes"].str.contains(r"[\S\s]*lakt[\S\s]*", case = False, regex = True), name = "probiotics_lakt")
    probiotics_ecoli = pd.Series(df["probiotics_notes"].str.contains(r"[\S\s]*coli[\S\s]*", case = False, regex = True), name = "probiotics_e_coli")

    return pd.concat([df, probiotics_b, probiotics_bifido, probiotics_ecoli, probiotics_f, probiotics_lakt, probiotics_m, probiotics_s, probiotics_t, probiotics_c], axis = 1)

def feeding_mode_conditional(row):

    mode = []
    if row['breastfed_baby1']:
        mode.append('breastfed')
    if row['formula_baby1']:
        mode.append('formula')
    if row['solids_baby1']:
           mode.append('solids')
    
    return mode

def edit_baby_feeding_mode(df: pd.DataFrame):

    # doing this for only baby1 under the assumption twins are fed identically
    temp_baby1 = pd.Series(df['food_baby1'].astype(str) + df['food_baby1_notes'].astype(str)) # this changes na values... 

    bf_baby1 = pd.Series(temp_baby1.str.contains(r'gestil', case = False), name = 'breastfed_baby1')
    formula_baby1 = pd.Series(temp_baby1.str.contains(r'milch|pre|aptamil', case = False, regex = True), name = 'formula_baby1')
    solids_baby1 = pd.Series(temp_baby1.str.contains(r'beikost|brei', case = False, regex = True), name = 'solids_baby1')

    df = pd.concat([df, bf_baby1, formula_baby1, solids_baby1], axis = 1)

    df['feeding_mode'] = df.apply(feeding_mode_conditional, axis = 1)

    return df

def baby_diet_condition(row):

    diet = []
    notes = row['special_diet_baby_notes'].lower()

    if 'fleisch' in notes:
        diet.append('less meat')
    if 'vege' in notes:
        diet.append('vegetarian')
    if 'vega' in notes:
        diet.append('vegan')
    if 'salz' in notes:
        diet.append('less salt')
    if 'zucker' in notes:
        diet.append('low sugar')
    if 'carb' in notes:
        diet.append('low carb')
    if 'eiwei' in notes:
        diet.append('little egg???')
    
    return diet

def edit_baby_diet(df: pd.DataFrame):

    # works under the assumption that diet with twins is equal!
    df['special_diet_baby_notes'] = df['food_baby1'].astype(str) + df['food_baby2'].astype(str) + df['food_baby1_notes'].astype(str) + df['food_baby2_notes'].astype(str) + df['diet_baby'].astype(str) + df['diet_baby_notes'].astype(str)
    df['special_diet_baby'] = df.apply(baby_diet_condition, axis = 1)

    return df

def replacing_values(df: pd.DataFrame):

    replace = ['.*[Jj][Aa].*', '.*[Nn][Ee][Ii][Nn].*', '.*[Jj]eden.*', '.*[Hh]öchstens.*', '.*[Mm]ehrmal.*']
    val = ['True', 'False', 'min. once per day', 'max. once per week', 'several times a week']
    
    for i in range(len(replace)):
        df = df.replace(to_replace = replace[i], regex = True, value = val[i])

    return df

def col_type_changes(df: pd.DataFrame):
    return df.convert_dtypes()

def edit_travel_time(df: pd.DataFrame):

    father = pd.Series((df['probe_date_mpi'] - df['probe_date_father']).dt.days, name = 'travel_time_mother')
    mother = pd.Series((df['probe_date_mpi'] - df['probe_date_mother']).dt.days, name = 'travel_time_father')
    sib1 = pd.Series((df['probe_date_mpi'] - df['probe_date_sib1']).dt.days, name = 'travel_time_sib1')
    sib2 = pd.Series((df['probe_date_mpi'] - df['probe_date_sib2']).dt.days, name = 'travel_time_sib2')
    baby1 = pd.Series((df['probe_date_mpi'] - df['probe_date_baby1']).dt.days, name = 'travel_time_baby1')
    baby2 = pd.Series((df['probe_date_mpi'] - df['probe_date_baby2']).dt.days, name = 'travel_time_baby2')


    return pd.concat([df, father, mother, sib1, sib2, baby1, baby2], axis = 1)

def removing_duplicates(df: pd.DataFrame):

    # for now hard-coded removal of duplicates/retaken samples 
    df = df[~((df['baby']=='B001')&(df['probe_date_mpi'].notna())&(df['time_point']=='9Monate'))]
    df = df[~((df['baby']=='B016')&(df['time_point']=='12Monate'))]
    df = df[~((df['baby']=='B027')&(df['time_point']=='12Monate'))]
    df = df[~((df['baby']=='B034')&(df['weight_mother']==75))] # note that this is a strange duplicate with fixed samples and variable samples separated, this is keeping the variable version

    return df

def clean_and_edit(df: pd.DataFrame):
    
    df = edit_baby_diet(df)
    df = edit_baby_feeding_mode(df)
    df = edit_probiotics(df)
    df = replacing_values(df)
    df = col_type_changes(df)
    df = edit_travel_time(df)
    df = removing_duplicates(df)

    return df

def save_baby_sheets(df: pd.DataFrame, path: str):
    
    today = date.today().strftime('%Y%m%d')
    df.to_csv(f'{path}{today}_baby_sheets.csv', index = False)