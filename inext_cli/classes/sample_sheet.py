import pandas as pd
import logging
from inext_cli.utils import guess_profile_format,file_valid,get_line_count


class sample_sheet:
    status = True
    def __init__(self, sample_sheet, id_col, metadata_cols=[], file_cols=[],skip_rows=0,restrict=False) -> None:
        self.status = file_valid(sample_sheet)
        self.skip_rows = skip_rows
        if not self.status:
            logging.critical("Error input file {} does not exists or is empty".format(sample_sheet))
            return
        
        line_count = get_line_count(sample_sheet)
        if line_count < 2:
            self.status = False
            logging.critical("Error input file {} is not valid since it has fewer than 2 lines".format(sample_sheet))
            return
        
        self.status = self.read_file(sample_sheet)
        if not self.status:
            self.status = False
            logging.critical("Error input file {} format was not recognized. Valid extensions: txt, text, tsv, csv, xls, xlsx".format(sample_sheet))
            return
        
        dup_cols = self.get_duplicate_columns(self.df)
        if len(dup_cols) > 0:
            self.status = False
            logging.critical("Error input file {} contains duplicated column names:{}, please remove them and try again".format(sample_sheet, dup_cols))
            return

        invalid_col_names = self.validate_field_names(list(self.df.columns))
        if len(invalid_col_names) > 0:
            self.status = False
            chars = [',',' ','"',"'",":"]
            logging.critical("Error input file {} contains column names: {} which have invalid characters {}, please remove them and try again".format(sample_sheet, invalid_col_names, chars))
            return

        cols_to_select = [id_col] + metadata_cols + file_cols
        missing_cols = self.get_missing_cols(cols_to_select)
        if len(missing_cols) > 0:
            self.status = False
            logging.critical("Error input file {} is missing the specified columns {}".format(sample_sheet,missing_cols))
            return

        if len(cols_to_select) > 1 and restrict:
            self.df = self.df[cols_to_select]

    def get_duplicate_columns(self,df):
        cols = list(df.columns)
        status = df.columns.duplicated()
        col_status = dict(map(lambda i,j : (i,j) , cols,status))
        results = []
        for col in col_status:
            if col_status[col]:
                results.append(cols)
        return results
        

    def read_file(self,f):
        file_format = guess_profile_format(f)       
        if file_format == 'excel':
            self.df =  pd.read_excel(f,sheet_name = 0, skiprows= self.skip_rows,dtype=str)
        elif file_format == 'tsv':
            self.df = pd.read_csv(f, header=0, sep="\t", skiprows= self.skip_rows,dtype=str)
        elif file_format == 'csv':
            self.df = pd.read_csv(f, header=0, sep=",", skiprows=self.skip_rows,dtype=str)
        else:
            self.df =  pd.DataFrame()
            return False
        self.df = self.df.fillna('')
        return True
    
    def get_missing_cols(self,cols):
        df_columns = set(self.df.columns)
        return sorted(list(set(cols) - df_columns))

    def validate_field_names(self,cols):
        results = []
        chars = [',',' ','"',"'",":"]
        for col in cols:
            status = True
            for char in chars:
                if char in col:
                    status = False
                    results.append(col)
                    break   
        self.status = status
        return results