import sqlite3 as sql
import datetime
import os
import colorama as col
import random


class Color:
	def __init__(self):
		self.colors = [
			'LIGHTCYAN_EX', 'MAGENTA', 'BLUE', 'LIGHTGREEN_EX', 'RED',
			'CYAN', 'LIGHTMAGENTA_EX', 'RESET', 'GREEN', 'LIGHTRED_EX', 'WHITE',
			'LIGHTWHITE_EX', 'YELLOW', 'LIGHTBLUE_EX', 'LIGHTYELLOW_EX'
		]
		
	def bg(self, color, text):
		color = color.upper()
		if color in self.colors:
			return eval('col.Back.'+color) + text + col.Back.RESET

	def fg(self, color, text):
		color = color.upper()
		if color in self.colors:
			return eval('col.Fore.'+color) + text + col.Fore.RESET

	def random(self, text):
		color = random.choice(self.colors)
		return eval('col.Fore.'+color) + text + col.Fore.RESET



class DB:
	def __init__(self, username, password, db_name="acc.db"):
		'''
			Database class to create, store, delete entries of our contents uploaded to mega from Google Drive

			Attributes
			-----------
				con : sqlite3 connection  object
				cur : cursor of sqlite3
				username : username of Mega
				password : password for username

			Methods:
				get_current_time : return time in YY-MM-DD HH:MM format
				commit: for commiting changes to db
				create_tables: create tables like Accounts, Contents, Access
				insert_account : insert entry into Account table
				insert_content : insert entry into Content table
				insert_access : insert entry into Access table
				get : print elements from table
				reset : remove all entries of table
				delete_dup : remove duplicates
				delete_by_pattern : delete matching pattern
				delete_row : remove row
				delete_table : delete table
				schema : print schema of table				
				delete_db : delete database
		'''
		self.con = sql.connect(db_name)
		self.cur = self.con.cursor()
		self.username = username
		self.password = password
		self.col = Color()
		self.create_tables()

	def run_sql(self, sql):
		'''Run sql statement'''
		if 'select' in sql.lower():
			return self.cur.execute(sql).fetchall()
		self.cur.execute(sql)
		self.commit()

	def commit(self, *cmds):
		'''commit changes to db'''
		for cmd in cmds:
			self.cur.execute(cmd)
		self.con.commit()

	def create_tables(self):
		'''
			Create accounts, contents, access tables
			Schema of
				Account : (username, password, creation_time)
				Content : (username, content)
				Access  : (username, last_access)
		'''
		account_sql = "CREATE TABLE IF NOT EXISTS Account(username varchar(30), \
			password varchar(30), creation_time date)"
		content_sql = "CREATE TABLE IF NOT EXISTS Content(username varchar(30), \
			content text)"
		access_sql = "CREATE TABLE IF NOT EXISTS Access(username varchar(30), \
			last_access date)"

		self.commit(account_sql,access_sql,content_sql)

	def insert_account(self, username=None, password=None):
		'''Insert entry(username, password, creation_time) into Account table '''
		if not username:
			username = self.username
		if not password:
			password = self.password
		
		cmd = f'INSERT INTO Account VALUES '
		cmd += f'("{username}", "{password}", "{self.get_current_time()}") '
		self.commit(cmd)

	def insert_content(self, username, *contents):
		'''Insert entry(username, content) into Content table '''
		cmd = f'INSERT INTO Content VALUES '
		for i in range(len(contents)):
			cmd += f'("{username}", "{contents[i]}")' + (', ' if i < len(contents)-1 else '')
		# print(cmd)
		self.commit(cmd)

	def insert_access(self, username, time=None):
		'''Insert entry(username, access_time) into Access table '''
		if time is None:
			time = self.get_current_time()
		cmd = f'INSERT INTO Access VALUES ("{username}", "{time}")'
		self.commit(cmd)

	def get(self, table):
		'''show contents of table'''
		cmd = f"SELECT * FROM {table}"
		order_by = ''
		if table == 'Account' or table == 'Access':
			order_by = ' order by username'
		else:
			order_by = ' order by content'
		cmd += order_by
		return self.cur.execute(cmd).fetchall()

	def search(self, name):
		'''search contents in Content'''
		words = "%' and content like '%".join(name.split())
		cmd = f"SELECT * FROM Content where content like '%{words}%' order by content"
		print(f"{self.col.fg('yellow', 'Username'):<30}  | {self.col.fg('Red', 'Content'):<100}")
		results = self.cur.execute(cmd).fetchall()
		no = 1
		cont = 1
		while cont != 'n':
			for res in results[no: no+10]:
				print(f"{no:<3} {self.col.fg('lightred_ex', res[0]):<30} | {self.col.fg('lightblue_ex', res[1]):<100}")
				no += 1
			cont = input('\nNext Page [y|n|q] : ')
			if no >= len(results) or cont == 'q':
				return results[:no]
		return results

	def reset(self, table):
		'''Delte every entires from table '''
		cmd = 'drop table ' + table
		self.commit(cmd)
		self.create_tables()

	def delete_dup(self, table=""):
		'''Delete duplicate entries from table'''
		cmd = ''
		if table:
			field = None
			if table=='Content':
				field="content"
			if table=='Account':
				field="password"
			
			if table == 'Access':
				cmd = f'''delete from {table} where rowid not in
					( select  min(rowid) from {table} group by username)'''
				return self.commit(cmd)
			
			cmd = f'''delete from {table} where rowid not in
					( select  min(rowid) from {table} group by username, {field})'''
		
		else:
			cmd = f'''delete from Content where rowid not in
					( select  min(rowid) from Content group by username, content)'''
			self.commit(cmd)

			cmd = f'''delete from Account where rowid not in
					( select  min(rowid) from Account group by username, password)'''
			self.commit(cmd)
			cmd = f'''delete from Access where rowid not in
					( select  min(rowid) from Access group by username)'''
		self.commit(cmd)

	def delete_by_pattern(self, table, field, pattern):
		'''Delete entries from table matching given pattern'''
		cmd = f'''delete from {table} where {field} like '%{pattern}%\''''
		self.commit(cmd)

	def delete_row(self, table, field, value):
		'''Delete entry from table matching given condition'''
		cmd = f'''delete from {table} where {field} like '{value}\''''
		self.commit(cmd)

	def delete_table(self, name):
		'''Delete table'''
		cmd = f'''drop table if exists {name}'''
		self.commit(cmd)

	def delete_db(self, name):
		'''Delete database'''
		os.unlink(name)

	def get_row(self, table, field, cond):
		cmd = f'select * from {table} where {field} = "{cond}"'
		return self.cur.execute(cmd).fetchall()

	def table_exists(self, table):
		cmd = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
		return bool(self.cur.execute(cmd).fetchall())

	def schema(self, table):
		'''Print schema of given table'''
		cmd = f"pragma table_info({table})"
		return self.cur.execute(cmd).fetchall()

	def modify_access(self, username):
		cmd = f'UPDATE Access SET last_access = {self.get_current_time()} where username = {username}'

	@staticmethod
	def get_current_time():
		''' Returns timestamp in YY-MM-DD HH:MM format'''
		return datetime.datetime.today().strftime("%d-%m-%Y %H:%M")


def download(username, password , path, dl_dir):
	try:
		dl_path = dl_dir+'/'+path.split('/')[-1]
		cmd = f'megaget -u {username} -p {password} "/Root/{path}" --path "{dl_path}"'
		print('Downloading ' + path)
		os.system(cmd)
	except:
		print('caused error')
		print(cmd)


if __name__ == '__main__':
	file  = input("Account File [accounts.db] : ")
	file = 'accounts.db' if  file == '' else file
	db = DB('', '', file)
	options = '1. Search [default]'
	print(options)

	opt = input()
	opt = 1 if opt == '' else int(opt)

	if opt == 1:
		while 1:
			key = input("Enter keywords : ")
			if key == 'q' or key == 'quit' or key =='':
				break
			results = db.search(key)
			print('Index to download')
			downlod_no = list(map(int, input().split()))
			if downlod_no == [0]:
				downlod_no = [i+1 for i in range(len(results))]

			dl_dir = input("Download Directory [~/Downloads/Mega Downloads] : ")
			if dl_dir == '':
				dl_dir = '/home/kcuf/Downloads/Mega Downloads'
			
			for no in downlod_no:
				download(results[no-1][0], 'password', results[no-1][1], dl_dir)

