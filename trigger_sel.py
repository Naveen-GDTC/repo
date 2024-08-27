import psycopg2
import pandas as pd 
import os 

stock_code = os.environ.get('STOCK_CODE')
current_dir = os.getcwd()

download_dir = current_dir
print("Files in download directory before wait:", os.listdir(download_dir))

files = os.listdir(download_dir)
excel_file = [i for i in files if i.endswith('.xlsx')]
print(excel_file)

df = pd.read_excel(excel_file[0], sheet_name='Data Sheet', header=14)
df = df.head(16)

df = df.rename(columns={'PROFIT & LOSS':''}).T
df.columns = df.iloc[0]
df = df.iloc[1:].fillna(0)
df = df.reset_index(drop = True)
for i in df.iloc[:,1:].columns:
    df[i] = df[i].astype('int')
df['stock'] =  stock_code

print(df)

db_params = {
    'dbname': 'reliance',
    'user': 'docker',
    'password': 'docker',
    'host': "192.168.1.188",
    'port': 5432
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

create_table_query= '''CREATE TABLE IF NOT EXISTS profit_loss_data_sel (
    report_date DATE,               
    sales INT ,                      
    raw_material_cost INT ,          
    change_in_inventory INT ,        
    power_and_fuel INT ,             
    other_mfr_exp INT ,              
    employee_cost INT ,             
    selling_and_admin INT ,          
    other_expenses INT ,            
    other_income INT ,              
    depreciation INT ,              
    interest INT ,                   
    profit_before_tax INT ,          
    tax INT ,                        
    net_profit INT ,                 
    dividend_amount INT,
    stock varchar(20)
);

CREATE TABLE IF NOT EXISTS profit_loss_data_sel_changes (
    change_id SERIAL PRIMARY KEY,
    operation TEXT,
    report_date DATE,
    sales INT,
    raw_material_cost INT,
    change_in_inventory INT,
    power_and_fuel INT,
    other_mfr_exp INT,
    employee_cost INT,
    selling_and_admin INT,
    other_expenses INT,
    other_income INT,
    depreciation INT,
    interest INT,
    profit_before_tax INT,
    tax INT,
    net_profit INT,
    dividend_amount INT,
    stock varchar(20)
);

CREATE OR REPLACE FUNCTION profit_loss_data_sel_changes() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO profit_loss_data_sel_changes (
            operation, report_date, sales, raw_material_cost, change_in_inventory, power_and_fuel,
            other_mfr_exp, employee_cost, selling_and_admin, other_expenses, other_income,
            depreciation, interest, profit_before_tax, tax, net_profit, dividend_amount,stock
        ) VALUES (
            'INSERT', NEW.report_date, NEW.sales, NEW.raw_material_cost, NEW.change_in_inventory,
            NEW.power_and_fuel, NEW.other_mfr_exp, NEW.employee_cost, NEW.selling_and_admin,
            NEW.other_expenses, NEW.other_income, NEW.depreciation, NEW.interest,
            NEW.profit_before_tax, NEW.tax, NEW.net_profit, NEW.dividend_amount, NEW.stock
        );
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO profit_loss_data_sel_changes (
            operation, report_date, sales, raw_material_cost, change_in_inventory, power_and_fuel,
            other_mfr_exp, employee_cost, selling_and_admin, other_expenses, other_income,
            depreciation, interest, profit_before_tax, tax, net_profit, dividend_amount,stock
        ) VALUES (
            'UPDATE', NEW.report_date, NEW.sales, NEW.raw_material_cost, NEW.change_in_inventory,
            NEW.power_and_fuel, NEW.other_mfr_exp, NEW.employee_cost, NEW.selling_and_admin,
            NEW.other_expenses, NEW.other_income, NEW.depreciation, NEW.interest,
            NEW.profit_before_tax, NEW.tax, NEW.net_profit, NEW.dividend_amount, NEW.stock
        );
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO profit_loss_data_sel_changes (
            operation, report_date, sales, raw_material_cost, change_in_inventory, power_and_fuel,
            other_mfr_exp, employee_cost, selling_and_admin, other_expenses, other_income,
            depreciation, interest, profit_before_tax, tax, net_profit, dividend_amount,stock
        ) VALUES (
            'DELETE', OLD.report_date, OLD.sales, OLD.raw_material_cost, OLD.change_in_inventory,
            OLD.power_and_fuel, OLD.other_mfr_exp, OLD.employee_cost, OLD.selling_and_admin,
            OLD.other_expenses, OLD.other_income, OLD.depreciation, OLD.interest,
            OLD.profit_before_tax, OLD.tax, OLD.net_profit, OLD.dividend_amount,OLD.stock
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
'''

cur.execute(create_table_query)
conn.commit()
print("table created")

trigger = '''
CREATE TRIGGER trigger_log_profit_loss_data_sel_changes
AFTER INSERT OR UPDATE OR DELETE ON profit_loss_data_sel
FOR EACH ROW
EXECUTE FUNCTION profit_loss_data_sel_changes();
'''
try:
    cur.execute(trigger)
    conn.commit()
    print("trigger created")
except psycopg2.Error as e:
    print(f"Error executing SQL commands: {e}")
    conn.rollback()
finally:
    pass

insert_query = '''INSERT INTO profit_loss_data_sel (
    report_date, sales, raw_material_cost, change_in_inventory, power_and_fuel, other_mfr_exp,              
    employee_cost, selling_and_admin, other_expenses, other_income, depreciation,              
    interest, profit_before_tax, tax, net_profit, dividend_amount,stock              
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''
for x, row in df.iterrows():
    cur.execute(insert_query, tuple(row))
    conn.commit() 
    print(f"Inserted data for year: {row['Report Date']}")
