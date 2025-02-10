#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' Extraction and transformation of the family metadata baby sheets (time point specific) from the babybiome project
    @Author: LRB
    @Date: 07.02.2025'''

import pandas as pd
import numpy as np
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
    baby = pd.Series(["Baby"] + [sheet] * (df.shape[0]-1))
    # timepoint col (categorical)
    df.iloc[:, 0] = df.iloc[:, 0].str.replace('Fragebogen ', "")
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