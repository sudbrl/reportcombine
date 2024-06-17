import streamlit as st
import pandas as pd
import dask.dataframe as dd
from openpyxl import load_workbook
from openpyxl.styles import Font
from io import BytesIO

def autofit_excel(writer):
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        for column_cells in worksheet.columns:
            max_length = 0
            column = column_cells[0].column_letter
            for cell in column_cells:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column].width = adjusted_width

def compare_excel_files(df_previous, df_this, writer):
    # Ensure that compulsory columns are present
    if 'Main Code' not in df_previous.columns or 'Balance' not in df_previous.columns:
        raise ValueError("Previous file is missing required columns: 'Main Code' and 'Balance'")
    if 'Main Code' not in df_this.columns or 'Balance' not in df_this.columns:
        raise ValueError("Current file is missing required columns: 'Main Code' and 'Balance'")

    # Exclude rows with Limit == 0 if the 'Limit' column is present
    if 'Limit' in df_previous.columns:
        df_previous = df_previous[df_previous['Limit'] != 0]
    if 'Limit' in df_this.columns:
        df_this = df_this[df_this['Limit'] != 0]

    # Filter out rows where 'Main Code' is 'AcType Total' or 'Grand Total'
    df_previous = df_previous[~df_previous['Main Code'].isin(['AcType Total', 'Grand Total'])]
    df_this = df_this[~df_this['Main Code'].isin(['AcType Total', 'Grand Total'])]

    previous_codes = set(df_previous['Main Code'])
    this_codes = set(df_this['Main Code'])

    only_in_previous = df_previous.loc[df_previous['Main Code'].isin(previous_codes - this_codes)]
    only_in_this = df_this.loc[df_this['Main Code'].isin(this_codes - previous_codes)]
    in_both = df_previous.loc[df_previous['Main Code'].isin(previous_codes & this_codes)]

    # Safe merge and calculation of balance changes
    in_both = pd.merge(
        in_both[['Main Code', 'Balance']], 
        df_this[['Main Code', 'Balance']], 
        on='Main Code', 
        suffixes=('_previous', '_this')
    )
    in_both['Change'] = in_both['Balance_this'] - in_both['Balance_previous']

    opening_sum = df_previous['Balance'].sum()
    settled_sum = only_in_previous['Balance'].sum()
    new_sum = only_in_this['Balance'].sum()
    increase_decrease_sum = in_both['Change'].sum()
    adjusted_sum = opening_sum - settled_sum + new_sum + increase_decrease_sum
    closing_sum = df_this['Balance'].sum()

    opening_count = len(previous_codes)
    settled_count = len(previous_codes - this_codes)
    new_count = len(this_codes - previous_codes)
    closing_count = len(this_codes)

    reco_data = {
        'Description': ['Opening', 'Settled', 'New', 'Increase/Decrease', 'Adjusted', 'Closing'],
        'Amount': [opening_sum, settled_sum, new_sum, increase_decrease_sum, adjusted_sum, closing_sum],
        'No of Acs': [opening_count, settled_count, new_count, "", "", closing_count]
    }
    df_reco = pd.DataFrame(reco_data)

    only_in_previous.to_excel(writer, sheet_name='Settled', index=False)
    only_in_this.to_excel(writer, sheet_name='New', index=False)
    in_both.to_excel(writer, sheet_name='Movement', index=False)
    df_reco.to_excel(writer, sheet_name='Reco', index=False)

def read_excel_sheets(file):
    sheets = pd.read_excel(file, sheet_name=None)
    dask_sheets = {sheet_name: dd.from_pandas(sheet_df, npartitions=1) for sheet_name, sheet_df in sheets.items()}
    return dask_sheets

def calculate_common_actype_desc(sheets_1, sheets_2, writer):
    common_actype_present = False
    for sheet_name in sheets_1:
        if sheet_name in sheets_2:
            df1 = sheets_1[sheet_name]
            df2 = sheets_2[sheet_name]

            # Ensure required columns exist
            if all(col in df1.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']) and \
               all(col in df2.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']):
                
                common_actype_present = True

                # Exclude rows with Limit == 0
                df1 = df1[df1['Limit'] != 0]
                df2 = df2[df2['Limit'] != 0]

                # Filter out rows where 'Main Code' is 'AcType Total' or 'Grand Total'
                df1 = df1[~df1['Main Code'].isin(['AcType Total', 'Grand Total'])]
                df2 = df2[~df2['Main Code'].isin(['AcType Total', 'Grand Total'])]

                # Group by 'Ac Type Desc' and calculate sum and count
                df1_grouped = df1.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'}).compute()
                df2_grouped = df2.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'}).compute()
                
                # Rename columns with appropriate names for previous and new sheets
                df1_grouped.columns = ['Previous Balance Sum', 'Previous Count']
                df2_grouped.columns = ['New Balance Sum', 'New Count']
                
                # Merge dataframes on 'Ac Type Desc' with a full outer join
                combined_df = pd.merge(df1_grouped, df2_grouped, left_index=True, right_index=True, how='outer')
                
                # Replace NaN values with 0
                combined_df = combined_df.fillna(0)

                # Calculate percentage change
                combined_df['Percent Change'] = ((combined_df['New Balance Sum'] - combined_df['Previous Balance Sum']) / combined_df['Previous Balance Sum']) * 100
                
                # Add total row
                total_row = pd.DataFrame(combined_df.sum()).transpose()
                total_row.index = ['Total']
                combined_df = pd.concat([combined_df, total_row])
                
                # Select relevant columns for output
                result_df = combined_df.reset_index()
                
                # Write to Excel sheet 'Compare'
                result_df.to_excel(writer, sheet_name='Compare', index=False)

                # Apply bold font to the total row
                worksheet = writer.sheets['Compare']
                total_row_idx = len(result_df)  # Total row index (1-based)
                for col in range(len(result_df.columns)):  # Loop over columns
                    cell = worksheet.cell(row=total_row_idx + 1, column=col + 1)  # Adjust column index to 1-based
                    cell.font = Font(bold=True)

                    # Apply percentage format to Percent Change column in the Total row
                    if result_df.columns[col] == 'Percent Change':
                        total_value = total_row[result_df.columns[col]].values[0]
                        cell.value = '{:.2f}%'.format(total_value / 100 if total_value > 0 else 0)
                        cell.number_format = '0.00%'

    return common_actype_present

def main():
    st.title("Excel File Comparison Tool")

    st.write("Upload the previous period's Excel file and this period's Excel file to compare them. The Columns Required are Main Code and Balance. Get download link.")
    previous_file = st.file_uploader("Upload Previous Period's Excel File", type=["xlsx"])
    current_file = st.file_uploader("Upload This Period's Excel File", type=["xlsx"])

    if previous_file and current_file:
        with st.spinner("Processing..."):
            df_previous = pd.read_excel(previous_file)
            df_this = pd.read_excel(current_file)

            excel_sheets_1 = read_excel_sheets(previous_file)
            excel_sheets_2 = read_excel_sheets(current_file)

            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                compare_excel_files(df_previous, df_this, writer)
                common_actype_present = calculate_common_actype_desc(excel_sheets_1, excel_sheets_2, writer)

                autofit_excel(writer)
            
            if not common_actype_present:
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    compare_excel_files(df_previous, df_this, writer)
                    autofit_excel(writer)

            output.seek(0)

        st.download_button(
            label="Download Combined Comparison Output",
            data=output,
            file_name="combined_comparison_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()
