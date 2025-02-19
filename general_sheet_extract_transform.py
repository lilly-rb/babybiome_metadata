#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' Extraction and transformation of the family metadata general sheet (not time point specific) from the babybiome project
    @Author: LRB 
    @Date: 19.02.2025'''

import pandas as pd
from datetime import date
import openpyxl

from babysheet_extract_transform import merge_normal_notes, set_col_names, deleting_cols, diet_condition, col_type_changes

def load_general_sheet(path: str):
    return pd.read_excel(path, sheet_name = 'Fragebogen-allgemein+Geburt', header = None)

def load_general_sheet_notes(path: str):
    workbook = openpyxl.load_workbook(path)
    worksheet = workbook['Fragebogen-allgemein+Geburt']
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
    notes.iloc[0, 2:] = normal.iloc[0, 2:] # adds baby labels back

    return notes

def rough_clean_general(df: pd.DataFrame):

    df.drop(index = 1, axis = 0, inplace = True)
    df.drop(df.columns[0], axis = 1, inplace = True)
    df.dropna(how = 'all', axis = 0, inplace = True)
    df.dropna(how = 'all', axis = 1, inplace = True)

    return df.T

def add_baby_col(df: pd.DataFrame):
    
    df.drop('family_notes', axis = 1, inplace = True)
    df['family'] = df['family'].str.extract(r'(B\d{3})')

    return df

def prepare_general(path: str, renaming: str, deleting: str):
    
    df = merge_normal_notes(load_general_sheet(path), load_general_sheet_notes(path))
    df = rough_clean_general(df)
    df = set_col_names(df, renaming)
    df = deleting_cols(df, deleting)
    df = add_baby_col(df)

    return df

def smoking_conditional(row):

    if 'nie' in row:
        return 'never'
    elif 'nein' in row:
        return 'no'
    elif 'ja' in row:
        return 'yes'
    elif 'früher' in row:
        return 'previously'
    else:
        return pd.NA
    
def edit_smoking(df: pd.DataFrame):
    
    df['smoking_father_notes'] = df['smoking_father'].astype(str) + df['smoking_father_notes'].astype(str)
    df['smoking_father'] = df['smoking_father_notes'].apply(smoking_conditional)

    df['smoking_mother_notes'] = df['smoking_mother'].astype(str) + df['smoking_mother_notes'].astype(str)
    df['smoking_mother'] = df['smoking_mother_notes'].apply(smoking_conditional)

    return df

def edit_family_diet(df: pd.DataFrame):

    # works under the assumption that diet with twins is equal!
    df['special_diet_family_notes'] = df['diet_family'].astype(str) + df['diet_family_notes'].astype(str)
    df['special_diet_family'] = df['special_diet_family_notes'].apply(diet_condition)

    return df

def replacing_values_general(df: pd.DataFrame):
  
    replace = ['[Jj][Aa]', '[Nn][Ee][Ii][Nn]']
    val = ['True', 'False', 'min. once per day']
    
    for i in range(len(replace)):
        df = df.replace(to_replace = replace[i], regex = True, value = val[i])

    return df       

def edit_birth_weight(df: pd.DataFrame):

    df['weight_baby1_at_birth'] = df['weight_baby1_at_birth'] / 1000
    df['weight_baby2_at_birth'] = df['weight_baby2_at_birth'] / 1000
    
    return df

def clean_and_edit_general(df: pd.DataFrame):

    df = edit_smoking(df)
    df = edit_family_diet(df)
    df = edit_birth_weight(df)
    df = replacing_values_general(df)
    df = col_type_changes(df)

    return df

def save_general_sheet(df: pd.DataFrame, path: str):
    
    today = date.today().strftime('%Y%m%d')
    df.to_csv(f'{path}{today}_general_sheet.csv', index = False)