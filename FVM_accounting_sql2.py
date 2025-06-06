import duckdb as db
import datetime

from duckdb.value.constant import NullValue


class Accounting:
    # This class connects the accounting programm with the database,
    # aswell as define some core function for later use
    def __init__(self, db_file, sql_script_file=None):
        self.db_file = db_file
        self.conn = db.connect(db_file)
        self.cur = self.conn.cursor()

        # If a SQL script file is provided, execute it to set up the database schema
        if sql_script_file:
            self.execute_sql_script(sql_script_file)

    def execute_sql(self, sql_text, Para=None):
        # takes an sql query as input and returns the result of the query as Database entry
        # use .fetchone()/fetchall to get as entry as tuple/list of tuples or print it
        if Para:
            result = self.conn.sql(sql_text, params=Para)
        else:
            result = self.conn.sql(sql_text)

        # Return the result of the query
        return result

    def execute_sql_script(self, script_file):
        with open(script_file, 'r') as file:
            sql_script = file.read()
        self.cur.execute(sql_script)
        self.conn.commit()  # Commit the transaction after running the script

    def close(self):
        self.conn.close()


class Account(Accounting):
    # This class allows to add new accounts to the database, aswell as call up
    # informations on the accounts for transactions etc.
    def add_account(self, acc_name, saldo, acc_type):
        try:
            query = "INSERT INTO accounts (acc_name, saldo, acc_type) VALUES (?, ?, ?)"
            self.execute_sql(query, (acc_name, saldo, acc_type))
            self.conn.commit()
            return self.execute_sql("SELECT * FROM accounts")

        except db.duckdb.ConstraintException as e:
            print("Account already exists!")
            return self.execute_sql("SELECT * FROM accounts")

    def update_saldo(self, acc_name, new_saldo):
        # update saldo after a booking has been registered
        query = "UPDATE accounts SET saldo=? WHERE acc_name=?"
        self.execute_sql(query, (new_saldo, acc_name))
        self.conn.commit()

    def get_saldo(self, acc_name):
        query = "SELECT saldo FROM accounts WHERE acc_name=?"
        saldo = self.execute_sql(query, (acc_name,)).fetchall()
        return saldo[0][0] if saldo else None

    def get_all_accounts(self):
        query = "SELECT * FROM accounts"
        result = self.execute_sql(query).fetchall()
        print(f"All Accounts: {result}")  # Debugging: print out all accounts
        return result


class Booking(Account):
    # This class allows to add bookings to the database from regiestered accounts
    def __init__(self, db_file):
        super().__init__(db_file)

    def highest_id(self):
        query = "SELECT max(id) FROM bookings"
        result = self.execute_sql(query).fetchall()[0][0]
        if result == NullValue:
            update_query = "UPDATE bookings SET id = 1 WHERE id = NULL"
            self.execute_sql(update_query)
            self.conn.commit()
            return 0
        elif result == None:
            return 0
        else:
            return result

    def make_booking(self, debitor, creditor, amount, comment):
        # Ensure the accounts exist
        max_id = self.highest_id()
        #print(max_id)
        new_id = int(max_id + 1)
        saldo_deb = self.get_saldo(debitor)
        saldo_cred = self.get_saldo(creditor)

        if saldo_deb is None:
            print(f"Error: Debitor '{debitor}' not found")
            return
        if saldo_cred is None:
            print(f"Error: Creditor '{creditor}' not found")
            return

            # Print out current saldo values
        print(f"Saldo Debitor ({debitor}): {saldo_deb}")
        print(f"Saldo Creditor ({creditor}): {saldo_cred}")
        # Create query to add booking to booking table
        query_booking = """
                        INSERT INTO bookings (id, debitor, creditor, amount, comment, booking_date) 
                        VALUES (?,?,?,?,?,?)
                    """

        self.execute_sql(query_booking, (new_id, debitor, creditor, amount, comment, datetime.date.today()))

        # Update saldos
        self.update_saldo(debitor, saldo_deb - amount)
        self.update_saldo(creditor, saldo_cred + amount)

        self.conn.commit()

        # Return updated booking and account information
        print(self.execute_sql("SELECT * FROM bookings"))
        print(self.execute_sql("SELECT * FROM accounts"))
        return self.execute_sql("SELECT * FROM bookings").fetchall(), self.execute_sql(
            "SELECT * FROM accounts").fetchall()

    def get_all_bookings(self):
        query = "SELECT * FROM bookings"
        result = self.execute_sql(query).fetchall()
        print(f"All Bookings: {result}")
        return result

    def delete_booking(self, id):
        del_query = "DELETE FROM bookings WHERE id=?"
        self.execute_sql(del_query, (id,))
        self.conn.commit()
        print(f"Remaining bookings: {self.execute_sql("SELECT * FROM bookings")}")
        return self.execute_sql("SELECT * FROM bookings").fetchall()

    def clear_booking_table(self):
        clear_query = "DELETE FROM bookings"
        self.execute_sql(clear_query)
        self.conn.commit()
        return self.execute_sql("SELECT * FROM bookings").fetchall()



# Usage example:
accounting = Accounting('booking_try4.ddb', 'FVM_Accounting.sql')  # Pass SQL script path here
accounting.execute_sql_script('FVM_Accounting.sql')
account = Account('booking_try4.ddb')  # Account instance
account.add_account('Institut', 100000, 'passiv')
account.add_account('Kaffeekasse', 300, 'aktiv')
account.add_account('FVM Bank', 20000, 'aktiv')
account.add_account('PVK', 20000, 'aktiv')
account.get_all_accounts()
booking = Booking('booking_try4.ddb') # Booking instance
booking.clear_booking_table()
booking.make_booking('Institut', 'FVM Bank', 1000, 'Jahresbeitrag')
booking.make_booking('PVK', 'FVM Bank', 4500, 'PVK Einnahmen 2024')

accounting.close()  # Close the connection at the end
