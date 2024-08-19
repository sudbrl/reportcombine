import streamlit as st
import pandas as pd
import dask.dataframe as dd
from openpyxl import load_workbook
from openpyxl.styles import Font
from io import BytesIO

# Define your credentials
USERNAME = 'admin'
PASSWORD = '123Punte@'

# Authentication function
def authenticate(username, password):
    return username == USERNAME and password == PASSWORD

# Hides the main menu, footer, and header
hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def preprocess_dataframe(df):
    # List of loan types to exclude
    loan_types_to_exclude = [
        'STAFF SOCIAL LOAN', 'STAFF VEHICLE LOAN', 'STAFF HOME LOAN',
        'STAFF FLEXIBLE LOAN', 'STAFF HOME LOAN(COF)'
    ]
    
    # Normalize 'Ac Type Desc' by stripping whitespace and converting to uppercase
    df['Ac Type Desc'] = df['Ac Type Desc'].str.strip().str.upper()
    
    # Convert the exclusion list to uppercase to match the normalized 'Ac Type Desc'
    loan_types_to_exclude = [loan_type.upper() for loan_type in loan_types_to_exclude]
    
    # Filter out specified loan types
    df = df[~df['Ac Type Desc'].isin(loan_types_to_exclude)]
    
    # Remove rows where 'Limit' is 0
    df = df[df['Limit'] != 0]
    
    # Exclude rows with specific 'Main Code' values
    df = df[~df['Main Code'].isin(['AcType Total', 'Grand Total'])]
    
    return df

def autofit_excel(writer):
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        for column_cells in worksheet.columns:
            max_length = max((len(str(cell.value)) for cell in column_cells), default=0)
            adjusted_width = max_length + 2
            worksheet.column_dimensions[column_cells[0].column_letter].width = adjusted_width

def compare_excel_files(df_previous, df_this, writer):
    required_columns = ['Main Code', 'Balance']
    for col in required_columns:
        if col not in df_previous.columns or col not in df_this.columns:
            raise ValueError(f"Missing required column: '{col}'")

    df_previous = preprocess_dataframe(df_previous)
    df_this = preprocess_dataframe(df_this)

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
        'Amount': [opening_sum, -settled_sum, new_sum, increase_decrease_sum, adjusted_sum, closing_sum],
        'No of Acs': [len(previous_codes), -len(previous_codes - this_codes), len(this_codes - previous_codes), "", "", len(this_codes)]
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
                cell.number_format = '0.00'  # Ensure Change column is not in percentage format
    
    return common_actype_present

def generate_slippage_report(df_previous, df_this, writer):
    if 'Provision' in df_previous.columns and 'Provision' in df_this.columns:
        try:
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
            st.error(f"An error occurred in the slippage report: {e}")
    else:
        st.warning("Provision column missing in one or both files.")

def generate_loan_quality_summary(df_this, writer):
    df_this = preprocess_dataframe(df_this)

    # Create the pivot table
    loan_quality_summary = df_this.pivot_table(index='Ac Type Desc', columns='Provision', values='Balance', aggfunc='sum').fillna(0)

    # Add Grand Total row for columns
    loan_quality_summary.loc['Grand Total'] = loan_quality_summary.sum()

    # Calculate the sum for the 'Total' column
    loan_quality_summary['Total'] = loan_quality_summary.sum(axis=1)

    # Reorder the columns
    column_order = ['Good', 'WatchList', 'Substandard', 'Doubtful', 'Bad', 'Total']
    loan_quality_summary = loan_quality_summary.reindex(columns=column_order)

    # Reset index and write to Excel
    loan_quality_summary.reset_index().to_excel(writer, sheet_name='Loan Quality', index=False)

    # Bold font for the 'Grand Total' row
    worksheet = writer.sheets['Loan Quality']
    for col in range(len(loan_quality_summary.columns)):
        cell = worksheet.cell(row=len(loan_quality_summary) + 1, column=col + 2)  # +2 to account for 1-based index and Total column
        cell.font = Font(bold=True)

# Main function to run the Streamlit app
def main():
    st.title("Excel File Comparison Tool")

    # Authentication
    if 'authenticated' not in st.session_state:
        with st.form(key='login_form'):
            st.subheader("Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type='password')
            submit_button = st.form_submit_button("Login")

            if submit_button:
                if authenticate(username, password):
                    st.session_state.authenticated = True
                    st.success("Authenticated successfully!")
                else:
                    st.error("Invalid username or password.")
        return

    # Main content
    st.write("Upload the previous period's Excel file and this period's Excel file to compare them.")
    previous_file = st.file_uploader("Upload Previous Period's Excel File", type=["xlsx"])
    current_file = st.file_uploader("Upload This Period's Excel File", type=["xlsx"])

    if previous_file and current_file:
        st.markdown('<style>div.stButton > button { background-color: #0b0080; color: blue; font-weight: bold; }</style>', unsafe_allow_html=True)
        start_processing_button = st.button("Start Processing", key="start_processing_button", help="Click to start processing")

        if start_processing_button:
            with st.spinner("Processing... Please wait."):
                try:
                    previous_wb = load_workbook(previous_file)
                    current_wb = load_workbook(current_file)

                    if len(previous_wb.sheetnames) > 1 or len(current_wb.sheetnames) > 1:
                        st.error("Each workbook should only contain one sheet.")
                    else:
                        df_previous = pd.read_excel(previous_file)
                        df_this = pd.read_excel(current_file)

                        excel_sheets_1 = read_excel_sheets(previous_file)
                        excel_sheets_2 = read_excel_sheets(current_file)

                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            common_actype_present = calculate_common_actype_desc(excel_sheets_1, excel_sheets_2, writer)
                            
                            compare_excel_files(df_previous, df_this, writer)
                            generate_slippage_report(df_previous, df_this, writer)
                            generate_loan_quality_summary(df_this, writer)
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
