import pandas as pd
from difflib import SequenceMatcher
from sqlalchemy import create_engine, MetaData, Table, inspect, text
from tkinter import *
from tkinter import ttk, messagebox, filedialog
import re

def display_column_order(df):
    return df.columns.tolist()

def reorder_columns(df, new_order):
    if any(i >= len(df.columns) for i in new_order):
        raise IndexError(f"Invalid column index in new_order: {new_order}. The DataFrame has only {len(df.columns)} columns.")
    return df[df.columns[new_order]]

def get_sql_table_columns(engine, table_name):
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    return columns

def align_columns(df_columns, sql_columns):
    column_map = {}
    for df_col in df_columns:
        matches = [(sql_col, SequenceMatcher(None, df_col, sql_col).ratio()) for sql_col in sql_columns]
        best_match, score = max(matches, key=lambda x: x[1])
        if score >= 0.8:
            column_map[df_col] = best_match
        else:
            column_map[df_col] = None
    return column_map

def apply_column_mapping(df, column_map):
    for df_col, sql_col in column_map.items():
        if sql_col:
            df.rename(columns={df_col: sql_col}, inplace=True)
    return df

def handle_column_discrepancy(df, sql_columns):
    column_map = align_columns(df.columns, sql_columns)
    unmatched = [col for col, match in column_map.items() if match is None]
    if unmatched:
        print(f"The following columns do not match any SQL column: {unmatched}")
        user_input = messagebox.askyesno("Column Mismatch", "Do you want to proceed with these columns as-is?")
        if not user_input:
            print("Data import aborted.")
            return None
    df = apply_column_mapping(df, column_map)
    return df

def disable_foreign_keys(engine):
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

def enable_foreign_keys(engine):
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

def import_data_to_mysql(sheets, connection_string):
    engine = create_engine(connection_string)
    metadata = MetaData()
    disable_foreign_keys(engine)
    try:
        for sheet_name, df in sheets.items():
            sql_columns = get_sql_table_columns(engine, sheet_name)
            df = handle_column_discrepancy(df, sql_columns)
            if df is None:
                continue
            df.to_sql(sheet_name, con=engine, if_exists='append', index=False)
    finally:
        enable_foreign_keys(engine)

def generate_connection_string(db_config):
    return f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"

def browse_file():
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    bot_01_entry.delete(0, END)
    bot_01_entry.insert(0, file_path)

def load_sheets():
    excel_file = bot_01_entry.get()
    if not excel_file:
        messagebox.showerror("Error", "Please select an Excel file.")
        return
    xls = pd.ExcelFile(excel_file)
    global sheets
    sheets = {}
    for sheet_name in xls.sheet_names:
        sheets[sheet_name] = pd.read_excel(xls, sheet_name)
    display_sheet_info(sheets)

def display_sheet_info(sheets):
    sheet_info_label.config(text="Select an Excel file to load sheets and columns.")
    colums_order_label.config(text='Enter columns order')
    colums_order_entry.delete(0, END)
    global current_sheet_index
    current_sheet_index = 0
    display_current_sheet()

def display_current_sheet():
    sheet_name = list(sheets.keys())[current_sheet_index]
    df = sheets[sheet_name]
    columns = display_column_order(df)
    columns_listbox.delete(0, END)
    for col in columns:
        columns_listbox.insert(END, col)
    colums_order_label.config(text=f'Enter columns order for {sheet_name}')

def next_sheet():
    global current_sheet_index
    if current_sheet_index < len(sheets) - 1:
        current_sheet_index += 1
        display_current_sheet()
    else:
        messagebox.showinfo("Info", "All sheets processed.")

def reset_entries():
    bot_01_entry.delete(0, END)
    colums_order_entry.delete(0, END)
    columns_listbox.delete(0, END)
    global sheets
    sheets = {}
    global current_sheet_index
    current_sheet_index = 0

def send_data():
    db_config = {
        "host": host_entry.get(),
        "user": user_entry.get(),
        "password": pass_entry.get(),
        "database": dbname_enrty.get()
    }
    connection_string = generate_connection_string(db_config)
    for sheet_name, df in sheets.items():
        new_order = eval(colums_order_entry.get())
        try:
            df = reorder_columns(df, new_order)
        except IndexError as e:
            messagebox.showerror("Error", str(e))
            continue
        sheets[sheet_name] = df
    try:
        import_data_to_mysql(sheets, connection_string)
        messagebox.showinfo("Success", "Data imported successfully")
    except Exception as e:
        messagebox.showerror("Error", str(e))

excelTomysql = Tk()
excelTomysql.title('EXCEL TO MYSQL')
excelTomysql.geometry('480x620')
excelTomysql.configure(background='#ffffff')
excelTomysql.resizable(False, False)

dbInfo = Frame(excelTomysql, bg='#ffffff')
dbInfo.place(x=10, y=20, width=460, height=180)

exceInfo = Frame(excelTomysql, bg='#ffffff')
exceInfo.place(x=10, y=210, width=460, height=240)

sheetInfo = Frame(excelTomysql, bg='#0000ff')
sheetInfo.place(x=10, y=460, width=460, height=80)

labeldbInfo = LabelFrame(dbInfo, text="|  Database Informations  |",  font=('Monospace',13))
labeldbInfo.pack(fill="both", expand="yes")

labelexceInfo = LabelFrame(exceInfo, text="|  Excel File Info  |",  font=('Monospace',13))
labelexceInfo.pack(fill="both", expand="yes")

labesheetInfo = LabelFrame(sheetInfo, text="|  Columns Order  |",  font=('Monospace',13))
labesheetInfo.pack(fill="both", expand="yes")
labesheetInfo_label = Frame(labesheetInfo, bg='#f0f0f0')
labesheetInfo_label.place(x=10, y=5, width=430, height=40)

top_left_db_info_label = Frame(labeldbInfo, bg='#f0f0f0')
top_left_db_info_label.place(x=10, y=5, width=202, height=130)

top_right_db_info_entry = Frame(labeldbInfo, bg='#f0f0f0')
top_right_db_info_entry.place(x=230, y=5, width=205, height=130)

bot_left_excel_label = Frame(labelexceInfo, bg='#f0f0f0')
bot_left_excel_label.place(x=10, y=5, width=205, height=80)

bot_right_excel_entry = Frame(labelexceInfo, bg='#f0f0f0')
bot_right_excel_entry.place(x=230, y=5, width=210, height=80)

host_label = Label(top_left_db_info_label, text='host', bg='#ffcc66', fg='#000000', font=('Monospace',13, 'bold'))
host_label.place(x=0, y=0, width=200, height=30)

user_label = Label(top_left_db_info_label, text='user', bg='#ffcc66', fg='#000000', font=('Monospace',13, 'bold'))
user_label.place(x=0, y=32, width=200, height=30)

pass_label = Label(top_left_db_info_label, text='password', bg='#ffcc66', fg='#000000', font=('Monospace',13, 'bold'))
pass_label.place(x=0, y=65, width=200, height=30)

dbname_label = Label(top_left_db_info_label, text='database', bg='#ffcc66', fg='#000000', font=('Monospace',13, 'bold'))
dbname_label.place(x=0, y=98, width=200, height=30)

host_entry = Entry(top_right_db_info_entry, bg='#ffffff', bd="1", justify='center', font=('Monospace',12))
host_entry.place(x=0, y=0, width=200, height=30)

user_entry = Entry(top_right_db_info_entry, bg='#ffffff', bd="1", justify='center', font=('Monospace',12))
user_entry.place(x=0, y=32, width=200, height=30)

pass_entry = Entry(top_right_db_info_entry, bg='#ffffff', bd="1", justify='center', font=('Monospace',12))
pass_entry.place(x=0, y=65, width=200, height=30)

dbname_enrty = Entry(top_right_db_info_entry, bg='#ffffff', bd="1", justify='center', font=('Monospace',12))
dbname_enrty.place(x=0, y=98, width=200, height=30)

excel_file = Label(bot_left_excel_label, text='Excel File', bg='#ffcc66', fg='#000000', font=('Monospace',13, 'bold'))
excel_file.place(x=0, y=2, width=200, height=30)

bot_01_entry = Entry(bot_right_excel_entry,  bg='#ffffff', bd="1", justify='center', font=('Monospace',12))
bot_01_entry.place(x=5, y=2, width=200, height=30)

button1_frame = Frame(exceInfo, bg='#f0f0f0')
button1_frame.place(x=5, y=70, width=450, height=160)

Load_Sheets = Button(button1_frame, text='Browse', bg='#415a77', fg='#ffffff', justify='center', font=('Monospace',13), cursor='hand2', command=browse_file)
Load_Sheets.place(x=101, y=5, width=120, height=35)

chse_file = Button(button1_frame, text='Load Sheets', bg='#415a77', fg='#ffffff', justify='center', font=('Monospace',13), cursor='hand2', command=load_sheets)
chse_file.place(x=228, y=5, width=120, height=35)

sheet_info_label = Label(button1_frame, text="Select an Excel file to load sheets and columns.", bg='#f0f0f0', fg='#000000', font=('Monospace', 12))
sheet_info_label.place(x=10, y=60, width=430, height=35)

columns_listbox = Listbox(button1_frame, bg='#ffffff', bd="1", font=('Monospace',12))
columns_listbox.place(x=10, y=100, width=430, height=50)

colums_order_label = Label(labesheetInfo_label, text='Enter columns order', bg='#ffcc66', fg='#000000', font=('Monospace',10))
colums_order_label.place(x=10, y=2, width=240, height=30)

colums_order_entry = Entry(labesheetInfo_label,  bg='#ffffff', bd="1", justify='center', font=('Monospace',12))
colums_order_entry.place(x=260, y=2, width=160, height=30)

button_frame = Frame(excelTomysql, bg='#ffffff')
button_frame.place(x=10, y=550, width=460, height=60)

send_butt = Button(button_frame, text='Envoyer', bg='#00ff00', fg='#000000', justify='center', font=('Monospace',15), cursor='hand2', command=send_data)
send_butt.place(x=250, y=10, width=100, height=40)

delet_butt = Button(button_frame, text='Effacer', bg='#808080', fg='#000000', justify='center', font=('Monospace',15), cursor='hand2', command=reset_entries)
delet_butt.place(x=360, y=10, width=100, height=40)

next_butt = Button(button_frame, text='Continue', bg='#415a77', fg='#ffffff', justify='center', font=('Monospace',15), cursor='hand2', command=next_sheet)
next_butt.place(x=140, y=10, width=100, height=40)

excelTomysql.mainloop()
