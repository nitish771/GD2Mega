import subprocess as sp
import os
import sys
import re
from math import ceil
from pprint import pprint
from collections import defaultdict as Dict
import sqlite3 as sql
import datetime
from random import shuffle

from Database import DB

db_name = 'accounts.db'


class Mega:
	"""
	Different actions for mega
	"""
	def __init__(self, username=None, password=None, local_folder="",
					remote_folder="", db_name=db_name):
		"""inital actions for generating veriables"""
		self.local_folder = local_folder
		self.remote_folder = remote_folder
		self.folder_name = self.local_folder.split("/")[-1]
		if not remote_folder:
			self.remote_folder = self.folder_name

		if local_folder and remote_folder:
			print("Folder : ", self.folder_name)
			print("Size : ", self.get_size())
	
		self.remote_mega_folder_path = remote_folder + "/" + self.folder_name
		self.username = username
		self.password = password
		self.accounts_file = "mega_accounts.csv"
		self.db = DB(self.username, self.password, db_name)

		# lists of data
		self.all_local_dirs = set()
		self.all_local_files = set()
		self.folds_created = {'/Root'}
			
	def start(self, register=False, show_size=True, group=None, skip_hidden=True):
		if register:
			self.register()
			self.db.insert_account(self.username, self.password)

		self.db.insert_account(self.username, self.password)

		# Upload
		if group:
			self.upload_files_from_group(group)
			self.db.delete_dup()
			print(f'\n\n{"	" + self.local_folder + " UPLOADED	":X^100}\n\n')
			return

		if show_size:
			print(f"Size : {self.folder_size:>8} | Folder : {self.folder_name:<50}")

		self.get_local_contents()
		groups = self.find_files_lte()

		for group in groups:
			self.upload_files_from_group(group)
		self.db.delete_dup()
		print(f'\n\n{"	" + self.local_folder + " UPLOADED	":X^100}\n\n')

	def equalize_name_for_mega(self, file_path):
		"""remove folder path from file path"""
		mega_path = self.remote_folder
		mega_path = ('/' + mega_path) if mega_path else mega_path
		return file_path.replace(self.local_folder, '"/Root' + mega_path)+ '"'

	def save_account(self, username, pass_, file=None):
		"""Save data to csv file"""
		if file is None:
			file = self.accounts_file
		print("saved ", username, 'to', file)

		with open(file, "a+") as f:
			f.write(username + ":" + pass_ + "\n")

	def register(self):
		"""Register new account and verify"""
		# Register
		try:
			cmd = "megareg --register -n nitish -e " + self.username + " -p " + self.password
			output = sp.check_output(cmd.split(" "))
			print("verification link sent to " + self.username)

			# Verify
			try:
				link = input("Enter confirmation link : ")
				verify_cmd = re.search("megareg --verify [\S]* ", str(output)).group(0) + link
				result = sp.check_output(verify_cmd.split(" "))
				print(result.decode('utf-8'))
				self.save_account(self.username, self.password)
			except:
				print("Error verifying link " + link)
		except:
			print(self.username + " : Already registered")

	def readable_size(self, size):
		"""convert bytes to human readable format"""
		size /= 1024
		if size < 1024:
			return f"{size:.2f} KB"
		size /= 1024
		if size < 1024:
			return f"{size:.2f} MB"
		size /= 1024
		return f"{size:.3f} GB"

	def get_local_contents(self, folder=None, skip_hidden=True):
		'''Get all the dirs, sub dirs to create and files to upload'''
		if folder is None:
			folder = self.local_folder
		for root, dirs, files in os.walk(folder):
			files = [f for f in files if not f[0] == '.']
			dirs[:] = [d for d in dirs if not d[0] == '.']

			for file in files:
				self.all_local_files.add(root + "/" + file)
			for dir_ in dirs:
				if skip_hidden:
					if dir_.startswith('.'):
						continue
				self.all_local_dirs.add(root + "/" + dir_)

	def create_mega_folds(self):
		'''call create_mega_dir for all the dirs in self.all_local_dirs'''
		self.create_mega_parent_dirs()
		for fold in self.all_local_dirs:
			self.create_mega_dir(fold)

	def create_mega_dir(self, path):
		'''Create mega directory'''
		cmd = "megamkdir -u " + self.username + " -p " + self.password
	
		if path in self.folds_created:
			return
		
		self.folds_created.add(path)
		path = self.double_quote_path(path)

		try:
			output = os.system(cmd + " " + path)
		except Exception as e:
			print(e)

	def create_mega_parent_dirs(self, path):
		'''create parent and sub directory for all the files in the group'''
		new_path = '/'
		for part in path[2:-1].split("/"):
			new_path = os.path.join(new_path, part)
			try:
				self.create_mega_dir(new_path)
			except Exception as e:
				print(e)

	def upload_files_from_group(self, group):
		'''iterate through files to upload in group'''
		
		print('group size : ', group[0], 'GB')
		for dir_ in sorted(group[1]):
			self.create_mega_parent_dirs(dir_)

		# shuffle list to reduce duplicacy possibility in case of multiprocessing
		shuffle(group[2])

		for item in group[2]:
			for path, new_path in item.items():
				self.upload_file(path, new_path)

	def upload_file(self, path, new_path):
		'''upload file to mega'''
		try:
			cmd = f"megaput -u {self.username} -p {self.password}"
			print("Uploading  : " + new_path.replace('/Root/', ''))
			cmd += ' --path ' + new_path + ' ' + self.double_quote_path(path)
			os.system(cmd)
			self.db.insert_content(self.username, self.remove_root_from_path(new_path))
		except Exception as e:
			print('File exists : ' + new_path)

	def account_details(self, email=None, password=None, print_res=True):
		'''Get details of mega account like used, free, total space'''
		if email is None:
			email = self.username
		if password is None:
			password = self.password

		cmd = f"megadf -u {email} -p {password}"
		output = str(sp.check_output(cmd.split(" ")))
		total = int(re.search("Total:\s*(\d+)", output).group(1))
		used =  int(re.search("Used:\s*(\d+)", output).group(1))
		free =  int(re.search("Free:\s*(\d+)", output).group(1))
		used_space = (col.Fore.RED + self.readable_size(used) + col.Fore.RESET) if used >= 21474836480 else self.readable_size(used)
		output = (
			  email + " -> Total : " + self.readable_size(total) +
			  " | Used  : " + used_space +
			  " | Free  : " + self.readable_size(free)
			)
		if print_res:
			print(output)
		if email:
			if self.db.get_row('access', 'username', email):
				self.db.modify_access(email)
			else:
				self.db.insert_access(email)
		return (self.size_in_gb(byts=total), self.size_in_gb(byts=used),
					self.size_in_gb(byts=free), output)

	def check_all_accounts_details(self, accounts=None, account_file=None):
		'''check details of all account in given list or file'''
		if accounts:
			for email, password in accounts.items():
				self.account_details(email, password)
			return

		if account_file:
			with open(account_file, "r") as f:
				for line in f.readlines():
					email, password = line.split(":")
					self.account_details(email, password.strip())

	def get_size(self, folder=None):
		'''return size of given content in readable format'''
		if folder == None:
			folder = self.local_folder
		return self.readable_size(self.folder_size(folder))

	def find_files_lte(self, capacity=19.5, sizes=None):
		'''create groups of files which takes 19.5 gb (default) or less'''
		cur_size = 0
		groups = []
		local_group = [set(), []]
		files = sorted(self.all_local_files)
		i = 0
		n = len(files)

		while i < n:
			file = files[i]
			cur_size += self.size_in_gb(file)

			if cur_size > capacity:
				cur_size -= self.size_in_gb(file)
				local_group.insert(0, round(cur_size,3))
				groups.append(local_group)
				local_group = [set(), []]
				cur_size = 0
				continue
				
			local_group[0].add(self.equalize_name_for_mega(
						self.get_file_dir(file)))  # dirs, subdirs
			local_group[1].append({file: self.equalize_name_for_mega(file)})
			i += 1

		if i == n:  # end of list of files
			local_group.insert(0, round(cur_size,3))
			groups.append(local_group)
		return groups
	
	def get_free_space(self, username, password):
		'''returns free space in mega account in gb'''
		return self.account_details(username, password, print_res=False)[2]  # total, used, free

	@staticmethod
	def remove_mega_content(start, end):
		for i in range(start, end+1):
			username = f'mega.nitish.{i:0>2}@hi2.in'
			cmd_ls = f'megals -u {username} -p password'
			cmd_rm = f'megarm -u {username} -p password'
			output = str(sp.check_output(cmd_ls.split()))
			ptrn_root = re.compile(r'(/Root/.*?)/|$|\\\\n')
			ptrn_trash = re.compile(r'(/Trash/.*?)/|$|\\n')
			folds = set()

			for j in ptrn_root.findall(output):
				if '\\n' in j:
					j = j.replace('\\n', '')
				folds.add(j)
			for j in ptrn_trash.findall(output):
				if '\\n' in j:
					j = j.replace('\\n', '')
				folds.add(j)
			print(username)
			for fold in folds:
				try:
					print('\tX', fold)
					os.system(cmd_rm + f' "{fold}"')
				except Exception as e:
					print(e)

	@staticmethod
	def double_quote_path(path):
		return '"' + path + '"'

	@staticmethod
	def remove_root_from_path(file):
		return file[1:-1].replace('/Root/', '')  # remove " from path then replace root with ''

	@staticmethod
	def get_file_dir(file):
		return os.path.dirname(file)

	@staticmethod
	def size_in_gb(fold=None, byts=None):
		if byts is None:
			byts = Mega.folder_size(fold)
		return round(byts / pow(1024, 3), 3)

	@staticmethod
	def size_in_mb(fold):
		byts = Mega.folder_size(fold)
		return round(byts / pow(1024, 2), 2)

	@staticmethod
	def folder_size(folder):
		'''get size in bytes'''
		size = 0
		if os.path.isfile(folder):
			return os.stat(folder).st_size
		for root, dirs, files in os.walk(folder):
			for file in files:
				size += os.stat(root + "/" + file).st_size
		return size


def upload(folds, mega_folder, email, password, first=False):
    for folder in folds:
        mega = Mega(folder, mega_folder, email, password)
        print(mega.get_size(folder), '\n')

        if first:
          mega.register_verify()
          first = False
        mega.start()
