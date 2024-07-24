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

def autofit_excel(writer):
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        for column_cells in worksheet.columns:
            max_length = max((len(str(cell.value)) for cell in column_cells), default=0)
            adjusted_width = max_length + 2
            worksheet.column_dimensions[column_cells[0].column_letter].width = adjusted_width

def preprocess_dataframe(df):
    loan_types_to_exclude = [
        'STAFF SOCIAL LOAN', 'STAFF VEHICLE LOAN', 'STAFF HOME LOAN',
        'STAFF FLEXIBLE LOAN', 'STAFF HOME LOAN(COF)'
    ]
    df['Ac Type Desc'] = df['Ac Type Desc'].str.strip().str.upper()
    loan_types_to_exclude = [loan_type.upper() for loan_type in loan_types_to_exclude]
    df = df[~df['Ac Type Desc'].isin(loan_types_to_exclude)]
    df = df[df['Limit'] != 0]
    df = df[~df['Main Code'].isin(['AcType Total', 'Grand Total'])]
    return df

def compare_excel_files(df_previous, df_this, writer):
    required_columns = ['Main Code', 'Balance']
    for col in required_columns:
        if col not in df_previous.columns or col not in df_this.columns:
            raise ValueError(f"Missing required column: '{col}'")

    df_previous = preprocess_dataframe(df_previous)
    df_this = preprocess_dataframe(df_this)
    df_previous['Main Code'] = df_previous['Main Code'].astype(str)
    df_this['Main Code'] = df_this['Main Code'].astype(str)

    previous_codes = set(df_previous['Main Code'])
    this_codes = set(df_this['Main Code'])
    only_in_previous = df_previous.loc[df_previous['Main Code'].isin(previous_codes - this_codes)]
    only_in_this = df_this.loc[df_this['Main Code'].isin(this_codes - previous_codes)]
    in_both = pd.merge(
        df_previous[['Main Code', 'Balance']],
        df_this[['Main Code','Branch Name', 'Name', 'Ac Type Desc', 'Balance']],
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

    reco_data = {
        'Description': ['Opening', 'Settled', 'New', 'Increase/Decrease', 'Adjusted', 'Closing'],
        'Amount': [opening_sum, settled_sum, new_sum, increase_decrease_sum, adjusted_sum, closing_sum],
        'No of Acs': [len(previous_codes), len(previous_codes - this_codes), len(this_codes - previous_codes), "", "", len(this_codes)]
    }
    df_reco = pd.DataFrame(reco_data)

    only_in_previous.to_excel(writer, sheet_name='Settled', index=False)
    only_in_this.to_excel(writer, sheet_name='New', index=False)
    in_both[['Main Code', 'Ac Type Desc', 'Branch Name', 'Name', 'Balance_this', 'Balance_previous', 'Change']].to_excel(writer, sheet_name='Movement', index=False)
    df_reco.to_excel(writer, sheet_name='Reco', index=False)

def read_excel_sheets(file):
    sheets = pd.read_excel(file, sheet_name=None)
    return {sheet_name: dd.from_pandas(sheet_df, npartitions=1) for sheet_name, sheet_df in sheets.items()}

def calculate_common_actype_desc(sheets_1, sheets_2, writer):
    common_actype_present = False
    combined_df = pd.DataFrame()
    for sheet_name_1, df1 in sheets_1.items():
        for sheet_name_2, df2 in sheets_2.items():
            if all(col in df1.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']) and \
               all(col in df2.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']):
                
                common_actype_present = True

                df1 = preprocess_dataframe(df1.compute())
                df2 = preprocess_dataframe(df2.compute())

                df1['Main Code'] = df1['Main Code'].astype(str)
                df2['Main Code'] = df2['Main Code'].astype(str)

                df1_grouped = df1.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'})
                df2_grouped = df2.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'})
                
                df1_grouped.columns = ['Previous Balance Sum', 'Previous Count']
                df2_grouped.columns = ['New Balance Sum', 'New Count']
                
                combined_df = pd.merge(df1_grouped, df2_grouped, left_index=True, right_index=True, how='outer').fillna(0)
                combined_df['Change'] = combined_df['New Balance Sum'] - combined_df['Previous Balance Sum']
                combined_df['Percent Change'] = ((combined_df['Change'] / combined_df['Previous Balance Sum'].replace({0: pd.NA})) * 100).fillna(0).map('{:.2f}%'.format)

                total_row = pd.DataFrame(combined_df.sum()).transpose()
                total_row.index = ['Total']
                total_prev_balance = total_row.at['Total', 'Previous Balance Sum']
                total_new_balance = total_row.at['Total', 'New Balance Sum']
                overall_percent_change = (total_new_balance - total_prev_balance) / total_prev_balance * 100 if total_prev_balance != 0 else 0
                total_row.at['Total', 'Percent Change'] = '{:.2f}%'.format(overall_percent_change)

                combined_df = pd.concat([combined_df, total_row])
                
    if common_actype_present:
        combined_df.reset_index().to_excel(writer, sheet_name='Compare', index=False)
        worksheet = writer.sheets['Compare']
        total_row_idx = len(combined_df)
        for col in range(len(combined_df.columns)):
            cell = worksheet.cell(row=total_row_idx + 1, column=col + 1)
            cell.font = Font(bold=True)
            if combined_df.columns[col] == 'Change':
                cell.number_format = '0.00'
    
    return common_actype_present

def generate_slippage_report(df_previous, df_this, writer):
    if 'Provision' in df_previous.columns and 'Provision' in df_this.columns:
        try:
            df_previous['Main Code'] = df_previous['Main Code'].astype(str)
            df_this['Main Code'] = df_this['Main Code'].astype(str)
            
            common_df = pd.merge(
                df_previous[['Main Code', 'Provision', 'Branch Name', 'Ac Type Desc', 'Name']],
                df_this[['Main Code', 'Balance', 'Provision']],
                on='Main Code',
                suffixes=('_Previous', '_This')
            )

            provision_pairs = [
                ('Good', 'WatchList'),
                ('WatchList', 'Substandard'),
                ('Good', 'Substandard'),
                ('Substandard', 'Doubtful'),
                ('Substandard', 'Bad'),
                ('WatchList', 'Doubtful'),
                ('Good', 'Doubtful'),
                ('Doubtful', 'Bad'),
                ('WatchList', 'Bad'),
                ('Good', 'Bad')
            ]

            filtered_df = common_df[
                common_df.apply(
                    lambda row: (row['Provision_Previous'], row['Provision_This']) in provision_pairs, axis=1
                )
            ][['Main Code', 'Name', 'Branch Name', 'Ac Type Desc', 'Balance', 'Provision_This', 'Provision_Previous']]

            filtered_df.to_excel(writer, sheet_name='Slippage', index=False)
        except Exception as e:
            st.write(f"Error generating Slippage report: {e}")

# Streamlit app interface
def main():
    st.title('Excel Comparison Tool')

    previous_file = st.file_uploader("Upload Previous Excel File", type=['xlsx'])
    this_file = st.file_uploader("Upload This Excel File", type=['xlsx'])

    if st.button('Compare Files'):
        if previous_file and this_file:
            try:
                sheets_1 = read_excel_sheets(previous_file)
                sheets_2 = read_excel_sheets(this_file)

                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    common_actype_desc_found = calculate_common_actype_desc(sheets_1, sheets_2, writer)
                    if not common_actype_desc_found:
                        st.write("No common 'Ac Type Desc' columns found in the uploaded files.")

                    for sheet_name_1, df1 in sheets_1.items():
                        for sheet_name_2, df2 in sheets_2.items():
                            compare_excel_files(df1.compute(), df2.compute(), writer)
                            generate_slippage_report(df1.compute(), df2.compute(), writer)
                    
                    autofit_excel(writer)

                st.success('Comparison completed. Download the result:')
                output.seek(0)
                st.download_button(label='Download Excel File', data=output, file_name='comparison_result.xlsx')
            except Exception as e:
                st.write(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
