import streamlit as st
import pandas as pd
import dask.dataframe as dd
from openpyxl import load_workbook
from openpyxl.styles import Font
from io import BytesIO

# Hides the main menu, footer, and header
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Function to adjust Excel column widths
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
                    if cell.value is None:
                        continue
                except Exception as e:
                    st.error(f"Error adjusting column width: {e}")
            adjusted_width = max_length + 2
            worksheet.column_dimensions[column].width = adjusted_width

# Function to preprocess dataframes: exclude specified loan types, filter rows based on 'Limit' and 'Main Code' criteria
def preprocess_dataframe(df):
    loan_types_to_exclude = [
        'STAFF SOCIAL LOAN', 'STAFF VEHICLE LOAN', 'STAFF HOME LOAN',
        'STAFF FLEXIBLE LOAN', 'STAFF HOME LOAN(COF)'
    ]
    if 'Ac Type Desc' in df.columns:
        df = df[~df['Ac Type Desc'].isin(loan_types_to_exclude)]

    if 'Limit' in df.columns:
        df = df[df['Limit'] != 0]

    if 'Main Code' in df.columns:
        df = df[~df['Main Code'].isin(['AcType Total', 'Grand Total'])]
    
    return df

# Function to compare two Excel files and generate a summary
def compare_excel_files(df_previous, df_this, writer):
    required_columns = ['Main Code', 'Balance']
    for col in required_columns:
        if col not in df_previous.columns:
            raise ValueError(f"Previous file is missing required column: '{col}'")
        if col not in df_this.columns:
            raise ValueError(f"Current file is missing required column: '{col}'")

    # Preprocess dataframes
    df_previous = preprocess_dataframe(df_previous)
    df_this = preprocess_dataframe(df_this)

    previous_codes = set(df_previous['Main Code'])
    this_codes = set(df_this['Main Code'])

    # Identify differences and calculate changes
    only_in_previous = df_previous.loc[df_previous['Main Code'].isin(previous_codes - this_codes)]
    only_in_this = df_this.loc[df_this['Main Code'].isin(this_codes - previous_codes)]
    in_both = df_previous.loc[df_previous['Main Code'].isin(previous_codes & this_codes)]

    in_both = pd.merge(
        in_both[['Main Code', 'Balance']],
        df_this[['Main Code', 'Balance']],
        on='Main Code',
        suffixes=('_previous', '_this')
    )
    in_both['Change'] = in_both['Balance_this'] - in_both['Balance_previous']

    # Calculate summary statistics
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

    # Prepare reconciliation data
    reco_data = {
        'Description': ['Opening', 'Settled', 'New', 'Increase/Decrease', 'Adjusted', 'Closing'],
        'Amount': [opening_sum, settled_sum, new_sum, increase_decrease_sum, adjusted_sum, closing_sum],
        'No of Acs': [opening_count, settled_count, new_count, "", "", closing_count]
    }
    df_reco = pd.DataFrame(reco_data)

    # Write results to Excel sheets
    only_in_previous.to_excel(writer, sheet_name='Settled', index=False)
    only_in_this.to_excel(writer, sheet_name='New', index=False)
    in_both.to_excel(writer, sheet_name='Movement', index=False)
    df_reco.to_excel(writer, sheet_name='Reco', index=False)

# Function to read Excel sheets into Dask DataFrames
def read_excel_sheets(file):
    sheets = pd.read_excel(file, sheet_name=None)
    dask_sheets = {sheet_name: dd.from_pandas(sheet_df, npartitions=1) for sheet_name, sheet_df in sheets.items()}
    return dask_sheets

# Function to compare 'Ac Type Desc' across Excel sheets and generate summary
def calculate_common_actype_desc(sheets_1, sheets_2, writer):
    common_actype_present = False
    for sheet_name_1 in sheets_1:
        for sheet_name_2 in sheets_2:
            df1 = sheets_1[sheet_name_1]
            df2 = sheets_2[sheet_name_2]

            if all(col in df1.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']) and \
               all(col in df2.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']):
                
                common_actype_present = True

                # Preprocess dataframes
                df1 = preprocess_dataframe(df1)
                df2 = preprocess_dataframe(df2)

                df1_grouped = df1.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'}).compute()
                df2_grouped = df2.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'}).compute()
                
                df1_grouped.columns = ['Previous Balance Sum', 'Previous Count']
                df2_grouped.columns = ['New Balance Sum', 'New Count']
                
                combined_df = pd.merge(df1_grouped, df2_grouped, left_index=True, right_index=True, how='outer')
                
                combined_df = combined_df.fillna(0)

                combined_df['Change'] = combined_df['New Balance Sum'] - combined_df['Previous Balance Sum']
                combined_df['Percent Change'] = ((combined_df['New Balance Sum'] - combined_df['Previous Balance Sum']) / combined_df['Previous Balance Sum']) * 100
                combined_df['Percent Change'] = combined_df['Percent Change'].apply(lambda x: '{:.2f}%'.format(x))

                total_row = pd.DataFrame(combined_df.sum()).transpose()
                total_row.index = ['Total']

                total_prev_balance = total_row.at['Total', 'Previous Balance Sum']
                total_new_balance = total_row.at['Total', 'New Balance Sum']
                overall_percent_change = ((total_new_balance - total_prev_balance) / total_prev_balance) * 100 if total_prev_balance != 0 else 0
                total_row.at['Total', 'Percent Change'] = '{:.2f}%'.format(overall_percent_change)

                # Exclude total row from sorting
                combined_df = combined_df[combined_df.index != 'Total']
                combined_df = combined_df.sort_values(by='Change', ascending=False)
                
                # Append total row back
                combined_df = pd.concat([combined_df, total_row])
                
                result_df = combined_df.reset_index()

                result_df.to_excel(writer, sheet_name='Compare', index=False)

                worksheet = writer.sheets['Compare']
                total_row_idx = len(result_df)
                for col in range(len(result_df.columns)):
                    cell = worksheet.cell(row=total_row_idx + 1, column=col + 1)
                    cell.font = Font(bold=True)

                    if result_df.columns[col] == 'Percent Change':
                        for row in range(2, total_row_idx + 2):
                            worksheet.cell(row=row, column=col + 1).number_format = '0.00%'

    return common_actype_present

# Function to generate slippage report
def generate_slippage_report(df_previous, df_this, writer):
    if 'Provision' in df_previous.columns and 'Provision' in df_this.columns:
        # Merge data on 'Main Code'
        common_df = pd.merge(
            df_previous[['Main Code', 'Provision', 'Branch Name', 'Ac Type Desc']],
            df_this[['Main Code', 'Balance', 'Provision']],
            on='Main Code',
            suffixes=('_Previous', '_This')
        )

        # Define the criteria for filtering
        provision_pairs = [
            ('Good', 'WatchList'),
            ('WatchList', 'Substandard'),
            ('Good', 'Substandard'),
            ('Bad', 'Doubtful'),
            ('Substandard', 'Doubtful'),
            ('WatchList', 'Doubtful'),
            ('Good', 'Doubtful'),
            ('Doubtful', 'Bad'),
            ('WatchList', 'Bad'),
            ('Good', 'Bad')
        ]

        # Filter based on provision pairs
        filtered_df = common_df[
            common_df.apply(
                lambda row: (row['Provision_Previous'], row['Provision_This']) in provision_pairs, axis=1
            )
        ][['Main Code', 'Balance', 'Provision_This', 'Provision_Previous', 'Branch Name', 'Ac Type Desc']]

        # Write to the new sheet
        filtered_df.to_excel(writer, sheet_name='Slippage', index=False)

# Main function to run the Streamlit app
def main():
    st.title("Excel File Comparison Tool")

    st.write("Upload the previous period's Excel file and this period's Excel file to compare them.")
    previous_file = st.file_uploader("Upload Previous Period's Excel File", type=["xlsx"])
    current_file = st.file_uploader("Upload This Period's Excel File", type=["xlsx"])

    if previous_file and current_file:
        st.markdown('<style>div.stButton > button { background-color: #0b0080; color: blue; font-weight: bold; }</style>', unsafe_allow_html=True)
        start_processing_button = st.button("Start Processing", key="start_processing_button", help="Click to start processing")

        if start_processing_button:
            with st.spinner("Processing Please Wait for a while..."):
                try:
                    df_previous = pd.read_excel(previous_file)
                    df_this = pd.read_excel(current_file)

                    excel_sheets_1 = read_excel_sheets(previous_file)
                    excel_sheets_2 = read_excel_sheets(current_file)

                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        compare_excel_files(df_previous, df_this, writer)
                        common_actype_present = calculate_common_actype_desc(excel_sheets_1, excel_sheets_2, writer)
                        generate_slippage_report(df_previous, df_this, writer)
                        autofit_excel(writer)

                    output.seek(0)
                    st.success("Processing completed successfully!")

                    st.download_button(
                        label="Download Comparison Sheet",
                        data=output,
                        file_name="combined_comparison_output.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error(f"An error occurred during processing: {e}")

if __name__ == "__main__":
    main()
